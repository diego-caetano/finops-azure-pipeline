"""
s07_consolidate_pbi.py — Consolida fact_allocated_cost diário no parquet Power BI.

Appenda o parquet do dia ao consolidado histórico e atualiza manifest.csv.
Idempotente: remove e reinsere a data a cada execução.

Saídas (gold/fact_allocated_cost_consolidated/):
  fact_allocated_cost_all.parquet   — histórico completo para Power BI
  manifest.csv                      — uma linha por data (Date desc)
"""

import gc
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from finops.storage import pipeline_client, upload_csv, upload_parquet
from finops.utils.dates import resolve_pipeline_date, to_partition

logger = logging.getLogger(__name__)

_FACT_PREFIX = os.getenv("FACT_ALLOCATED_COST_PREFIX", "gold/fact_allocated_cost")
_PBI_BLOB = os.getenv(
    "PBI_CONSOLIDATED_BLOB",
    "gold/fact_allocated_cost_consolidated/fact_allocated_cost_all.parquet",
)
_PBI_MANIFEST = os.getenv(
    "PBI_MANIFEST_BLOB",
    "gold/fact_allocated_cost_consolidated/manifest.csv",
)

_MANIFEST_COLUMNS = [
    "Date", "Status", "Rows", "TotalAllocated", "TotalCost", "ProcessedAt", "PipelineRun",
]


def run(pipeline_date: Optional[str] = None) -> None:
    date_str = resolve_pipeline_date(pipeline_date)
    logger.info("📅 s07_consolidate_pbi  date=%s", date_str)

    store = pipeline_client()
    storage_account = os.environ["PIPELINE_STORAGE_ACCOUNT"]
    container = os.environ["PIPELINE_CONTAINER"]
    pipeline_run = os.environ.get("PIPELINE_RUN_ID", "manual")
    processed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Captured early so the ERROR manifest entry is populated even on early failure
    n_day = 0
    total_day = 0.0
    total_cost_day = 0.0

    try:
        # ── Step 1: Lê parquet do dia ─────────────────────────────────────
        day_blob = (
            f"{_FACT_PREFIX}/{to_partition(date_str)}"
            f"/fact_allocated_cost_{date_str}.parquet"
        )
        if not store.exists(day_blob):
            msg = f"fact_allocated_cost para {date_str} não encontrado: {day_blob}"
            logger.error("❌ %s", msg)
            raise FileNotFoundError(msg)

        df_day = store.read_parquet(day_blob)
        n_day = len(df_day)
        total_day = float(df_day["AllocatedCost"].sum())
        logger.info("📊 parquet do dia: %d linhas | R$ %.2f alocados", n_day, total_day)

        # TotalCost vem da reconciliation (TotalFactCost = custo real sem duplicação do fan-out)
        recon_blob = (
            f"{_FACT_PREFIX}/{to_partition(date_str)}"
            f"/reconciliation_{date_str}.csv"
        )
        try:
            df_recon = store.read_csv(recon_blob)
            total_cost_day = float(df_recon["TotalFactCost"].iloc[0])
        except Exception:
            logger.warning("⚠ reconciliation não encontrada — TotalCost será 0")
            total_cost_day = 0.0

        # ── Step 2: Carrega consolidado existente ─────────────────────────
        try:
            df_all = store.read_parquet(_PBI_BLOB)
            logger.info("📊 consolidado existente: %d linhas", len(df_all))
        except Exception:
            logger.info("📊 consolidado não encontrado — iniciando vazio")
            df_all = pd.DataFrame(columns=df_day.columns)

        # ── Step 3: Remove linhas do dia (idempotência) ───────────────────
        before = len(df_all)
        df_all = df_all[df_all["Date"].astype(str) != date_str].copy()
        removed = before - len(df_all)
        if removed:
            logger.info("🗑  Removidas %d linhas antigas do dia %s", removed, date_str)

        # ── Step 4: Concat e grava ────────────────────────────────────────
        df_combined = pd.concat([df_all, df_day], ignore_index=True)
        del df_all
        gc.collect()

        upload_parquet(df_combined, storage_account, container, _PBI_BLOB)
        logger.info("⬆️  consolidado: %d linhas → %s", len(df_combined), _PBI_BLOB)

        # ── Step 5: Validações ────────────────────────────────────────────
        df_check = df_combined[df_combined["Date"].astype(str) == date_str]
        n_check = len(df_check)
        total_check = float(df_check["AllocatedCost"].sum())

        if n_check != n_day:
            raise ValueError(
                f"Validação falhou: linhas no consolidado={n_check}, esperado={n_day}"
            )
        if abs(total_check - total_day) >= 1.0:
            raise ValueError(
                f"Validação falhou: TotalAllocated consolidado={total_check:.6f}, "
                f"original={total_day:.6f}, diff={abs(total_check - total_day):.6f}"
            )

        n_total = len(df_combined)
        n_days = df_combined["Date"].astype(str).nunique()
        pct_var = _calc_day_variation(df_combined, date_str, total_day)

        del df_combined
        gc.collect()

        # ── Step 6: Manifest (OK) ─────────────────────────────────────────
        _update_manifest(
            store, storage_account, container,
            date_str, "OK", n_day, total_day, total_cost_day,
            processed_at, pipeline_run,
        )

        # ── Step 7: Log resumo ────────────────────────────────────────────
        logger.info(
            "✅ s07 — %s | %d linhas | R$ %.2f alocados"
            " | consolidado total: %d linhas (%d dias)"
            " | variação vs dia anterior: %+.1f%%",
            date_str, n_day, total_day, n_total, n_days, pct_var,
        )

    except Exception:
        logger.exception("❌ s07 falhou para %s", date_str)
        try:
            _update_manifest(
                store, storage_account, container,
                date_str, "ERROR", n_day, total_day, total_cost_day,
                processed_at, pipeline_run,
            )
        except Exception:
            logger.warning("⚠ Não foi possível atualizar manifest com STATUS=ERROR")
        raise


def _calc_day_variation(df: pd.DataFrame, date_str: str, total_day: float) -> float:
    """Variação percentual do AllocatedCost vs o dia imediatamente anterior no consolidado."""
    dates_sorted = sorted(df["Date"].astype(str).unique())
    try:
        idx = dates_sorted.index(date_str)
    except ValueError:
        return 0.0
    if idx == 0:
        return 0.0
    prev_date = dates_sorted[idx - 1]
    prev_total = float(df[df["Date"].astype(str) == prev_date]["AllocatedCost"].sum())
    if prev_total == 0.0:
        return 0.0
    return (total_day - prev_total) / prev_total * 100.0


def _update_manifest(
    store,
    storage_account: str,
    container: str,
    date_str: str,
    status: str,
    rows: int,
    total_allocated: float,
    total_cost: float,
    processed_at: str,
    pipeline_run: str,
) -> None:
    try:
        df_manifest = store.read_csv(_PBI_MANIFEST)
    except Exception:
        df_manifest = pd.DataFrame(columns=_MANIFEST_COLUMNS)

    df_manifest = df_manifest[df_manifest["Date"].astype(str) != date_str].copy()

    new_row = pd.DataFrame([{
        "Date": date_str,
        "Status": status,
        "Rows": rows,
        "TotalAllocated": round(total_allocated, 6),
        "TotalCost": round(total_cost, 6),
        "ProcessedAt": processed_at,
        "PipelineRun": pipeline_run,
    }], columns=_MANIFEST_COLUMNS)

    df_manifest = pd.concat([df_manifest, new_row], ignore_index=True)
    df_manifest = df_manifest.sort_values("Date", ascending=False).reset_index(drop=True)

    upload_csv(df_manifest, storage_account, container, _PBI_MANIFEST)
    logger.info("📋 manifest: %d entradas → %s", len(df_manifest), _PBI_MANIFEST)
