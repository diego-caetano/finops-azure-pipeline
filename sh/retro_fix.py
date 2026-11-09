#!/usr/bin/env python3
"""Retrocarga corretiva — reprocessa datas com FOCUS billing lag.

Uso:
  python sh/retro_fix.py                           # 7 dias terminando em D-3 (padrão CronJob)
  python sh/retro_fix.py --start 2026-05-15 --end 2026-05-20

Pode ser executado:
  - Localmente (requer az login com permissões no blob)
  - Como CronJob Kubernetes (usa Workload Identity automaticamente)

Executa os mesmos steps do pipeline principal (s01→s07) para cada data,
lendo o FOCUS mais recente do blob. Cobre billing lag de 2–3 dias e casos
extremos como RI, Savings Plans e virada de mês.
"""

import argparse
import ctypes
import gc
import io
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

BRT = timezone(timedelta(hours=-3))
DEFAULT_LOOKBACK_DAYS = 7


def _load_dotenv() -> None:
    env_file = _ROOT / ".env"
    if not env_file.exists():
        return
    with open(env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())


_load_dotenv()
os.environ.setdefault("PIPELINE_RUN_ID", "retro-fix")

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

from finops.storage import pipeline_client, upload_csv, upload_parquet  # noqa: E402
from finops.utils.dates import to_partition  # noqa: E402
from finops.steps import (  # noqa: E402
    s01_ingest_focus,
    s02_classify,
    s03a_dedicated,
    s03c_sql_pool,
    s03d_backup,
    s04_client_weights,
    s05_consolidate,
    s06_fact_allocated_cost,
)

_STEPS = [
    ("s01_ingest_focus",        s01_ingest_focus.run),
    ("s02_classify",            s02_classify.run),
    ("s03a_dedicated",          s03a_dedicated.run),
    ("s03c_sql_pool",           s03c_sql_pool.run),
    ("s03d_backup",             s03d_backup.run),
    ("s04_client_weights",      s04_client_weights.run),
    ("s05_consolidate",         s05_consolidate.run),
    ("s06_fact_allocated_cost", s06_fact_allocated_cost.run),
]

_PBI_CONSOLIDATED_PREFIX = os.getenv("PBI_CONSOLIDATED_PREFIX", "gold/fact_allocated_cost_consolidated")
_PBI_MANIFEST = os.getenv("PBI_MANIFEST_BLOB",          "gold/fact_allocated_cost_consolidated/manifest.csv")
_FACT_PREFIX  = os.getenv("FACT_ALLOCATED_COST_PREFIX", "gold/fact_allocated_cost")
_MANIFEST_COLS = ["Date", "Status", "Rows", "TotalAllocated", "TotalCost", "ProcessedAt", "PipelineRun"]


def _monthly_blob(date_str: str) -> str:
    return f"{_PBI_CONSOLIDATED_PREFIX}/fact_allocated_cost_{date_str[:7]}.parquet"


def _release_memory() -> None:
    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


def parse_args() -> tuple[str, str]:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--start", help="Data inicial YYYY-MM-DD (inclusive)")
    parser.add_argument("--end",   help="Data final   YYYY-MM-DD (inclusive)")
    args = parser.parse_args()

    # RETRO_START/RETRO_END allow Kubernetes Jobs to set the range via env vars
    start = args.start or os.environ.get("RETRO_START")
    end   = args.end   or os.environ.get("RETRO_END")

    if bool(start) != bool(end):
        parser.error("--start e --end devem ser usados juntos (ou RETRO_START/RETRO_END)")

    if start and end:
        return start, end

    # Janela [D-(LAG+6), D-LAG]: 7 dias contíguos entre execuções semanais, sem nunca
    # gravar um dia ainda aberto pelo billing lag. Antes era [D-7, D-1] e a execução de
    # segunda gravava D-1/D-2 parciais no consolidado (ex.: 08/11/2026 entrou com R$5,3k
    # em vez de ~R$11,5k; mesma causa da divergência de 57% em 23/08/2026).
    from finops.utils.dates import FOCUS_LAG_DAYS

    today = datetime.now(BRT)
    end   = (today - timedelta(days=FOCUS_LAG_DAYS)).strftime("%Y-%m-%d")
    start = (today - timedelta(days=FOCUS_LAG_DAYS + DEFAULT_LOOKBACK_DAYS - 1)).strftime("%Y-%m-%d")
    return start, end


def date_range(start: str, end: str) -> list[str]:
    s, e, dates = (
        datetime.strptime(start, "%Y-%m-%d"),
        datetime.strptime(end,   "%Y-%m-%d"),
        [],
    )
    cur = s
    while cur <= e:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return dates


def _read_manifest_antes(dates: list[str]) -> dict[str, float]:
    """Lê TotalAllocated do manifest atual para as datas do lote."""
    before: dict[str, float] = {}
    try:
        store = pipeline_client()
        df = store.read_csv("gold/fact_allocated_cost_consolidated/manifest.csv")
        for _, row in df.iterrows():
            d = str(row.get("Date", "")).strip()
            if d in dates:
                before[d] = float(row.get("TotalAllocated", 0))
    except Exception:
        logger.warning("⚠ Manifest não encontrado — valores 'antes' serão 0")
    return before


def _read_total_fact_cost(date_str: str) -> float:
    """Lê TotalFactCost da reconciliation gerada pelo s06."""
    try:
        store = pipeline_client()
        df = store.read_csv(
            f"gold/fact_allocated_cost/dt={date_str}/reconciliation_{date_str}.csv"
        )
        return float(df["TotalFactCost"].iloc[0])
    except Exception as exc:
        logger.warning("⚠ Não foi possível ler reconciliation de %s: %s", date_str, exc)
        return 0.0


def _rebuild_consolidated_batch(pass_dates: list[str]) -> None:
    """Rebuilds monthly PBI parquets + manifest in one pass per month.

    Groups dates by year-month so each monthly file is downloaded and uploaded
    exactly once, regardless of how many dates are in the batch.
    Retro batches that cross a month boundary produce two monthly uploads.
    """
    if not pass_dates:
        logger.info("⚠ Nenhuma data OK — skip consolidação")
        return

    store = pipeline_client()
    storage_account = os.environ["PIPELINE_STORAGE_ACCOUNT"]
    container = os.environ["PIPELINE_CONTAINER"]
    pipeline_run = os.environ.get("PIPELINE_RUN_ID", "retro-fix")
    processed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Group dates by year-month (retro que cruza virada de mês gera 2 uploads)
    from collections import defaultdict
    month_groups: dict[str, list[str]] = defaultdict(list)
    for d in sorted(pass_dates):
        month_groups[d[:7]].append(d)

    manifest_rows: list[dict] = []

    for year_month, dates_in_month in sorted(month_groups.items()):
        monthly_blob = _monthly_blob(dates_in_month[0])
        logger.info("📦 Consolidando %s (%d datas)...", year_month, len(dates_in_month))

        try:
            df_month = store.read_parquet(monthly_blob)
            logger.info("📊 consolidado mensal existente: %d linhas", len(df_month))
        except Exception:
            logger.info("📊 consolidado mensal não encontrado — iniciando vazio")
            df_month = pd.DataFrame()

        if not df_month.empty:
            df_month = df_month[~df_month["Date"].astype(str).isin(set(dates_in_month))].copy()

        day_frames: list[pd.DataFrame] = []
        for date_str in dates_in_month:
            day_blob = f"{_FACT_PREFIX}/{to_partition(date_str)}/fact_allocated_cost_{date_str}.parquet"
            try:
                df_day = store.read_parquet(day_blob)
            except Exception as exc:
                logger.warning("⚠ Parquet não encontrado para %s: %s", date_str, exc)
                continue
            n_day = len(df_day)
            total_day = float(df_day["AllocatedCost"].sum())

            recon_blob = f"{_FACT_PREFIX}/{to_partition(date_str)}/reconciliation_{date_str}.csv"
            try:
                df_recon = store.read_csv(recon_blob)
                total_cost = float(df_recon["TotalFactCost"].iloc[0])
            except Exception:
                total_cost = 0.0

            day_frames.append(df_day)
            manifest_rows.append({
                "Date": date_str, "Status": "OK", "Rows": n_day,
                "TotalAllocated": round(total_day, 6), "TotalCost": round(total_cost, 6),
                "ProcessedAt": processed_at, "PipelineRun": pipeline_run,
            })
            logger.info("  📅 %s | %d linhas | R$ %.2f", date_str, n_day, total_day)

        parts = ([] if df_month.empty else [df_month]) + day_frames
        if not parts:
            continue
        df_combined = pd.concat(parts, ignore_index=True)

        upload_parquet(df_combined, storage_account, container, monthly_blob)
        n_days_out = df_combined["Date"].astype(str).nunique()
        logger.info("⬆️  %s: %d linhas (%d datas) → %s", year_month, len(df_combined), n_days_out, monthly_blob)

    if manifest_rows:
        try:
            df_manifest = store.read_csv(_PBI_MANIFEST)
        except Exception:
            df_manifest = pd.DataFrame(columns=_MANIFEST_COLS)
        new_dates = {r["Date"] for r in manifest_rows}
        df_manifest = df_manifest[~df_manifest["Date"].astype(str).isin(new_dates)].copy()
        df_manifest = pd.concat(
            [df_manifest, pd.DataFrame(manifest_rows, columns=_MANIFEST_COLS)],
            ignore_index=True,
        )
        df_manifest = df_manifest.sort_values("Date", ascending=False).reset_index(drop=True)
        upload_csv(df_manifest, storage_account, container, _PBI_MANIFEST)
        logger.info("📋 manifest: %d entradas → %s", len(df_manifest), _PBI_MANIFEST)


def run_date(date_str: str) -> None:
    for name, step_fn in _STEPS:
        logger.info("▶️  %s", name)
        try:
            step_fn(date_str)
        except Exception:
            logger.exception("❌ %s falhou  date=%s", name, date_str)
            raise
        _release_memory()
        logger.info("✅ %s concluído", name)


def main() -> None:
    start, end = parse_args()
    dates = date_range(start, end)

    logger.info("🔄 retro-fix: %s → %s (%d datas)", start, end, len(dates))
    logger.info("   Motivo: FOCUS billing lag — reprocessamento com dados settlados")

    logger.info("📋 Lendo manifest atual (valores antes)...")
    before_map = _read_manifest_antes(dates)

    pass_count  = 0
    fail_count  = 0
    pass_dates: list[str] = []
    total_antes  = 0.0
    total_depois = 0.0

    for date_str in dates:
        logger.info("=" * 54)
        logger.info(">> %s", date_str)
        antes = before_map.get(date_str, 0.0)
        depois = 0.0
        try:
            run_date(date_str)
            depois = _read_total_fact_cost(date_str)
            diff_pct = ((depois - antes) / antes * 100) if antes > 0 else 0.0
            sign = "+" if diff_pct >= 0 else ""
            logger.info(
                "✅ %s | antes=%s  depois=%s  (%s%.1f%%)",
                date_str, f"{antes:,.2f}", f"{depois:,.2f}", sign, diff_pct,
            )
            pass_count  += 1
            pass_dates.append(date_str)
            total_antes  += antes
            total_depois += depois
        except Exception:
            logger.exception("❌ Falha em %s", date_str)
            fail_count += 1

    # Resumo final
    logger.info("=" * 54)
    logger.info("📊 Resumo retro-fix: %s → %s", start, end)
    logger.info("   Datas OK  : %d/%d", pass_count, len(dates))
    if fail_count:
        logger.warning("   Falhas    : %d", fail_count)
    if pass_count > 0:
        diff_total = total_depois - total_antes
        diff_pct   = (diff_total / total_antes * 100) if total_antes > 0 else 0.0
        sign = "+" if diff_total >= 0 else ""
        logger.info("   Total antes : %s", f"{total_antes:,.2f}")
        logger.info("   Total depois: %s", f"{total_depois:,.2f}")
        logger.info("   Diferença   : %s%s (%s%.1f%%)", sign, f"{diff_total:,.2f}", sign, diff_pct)

    # Rebuild PBI consolidated in one batch pass (replaces per-date s07 calls)
    logger.info("=" * 54)
    if os.environ.get("SKIP_CONSOLIDATED_REBUILD"):
        logger.info("⚠ SKIP_CONSOLIDATED_REBUILD=1 — consolidação PBI ignorada (modo validação)")
    else:
        _rebuild_consolidated_batch(pass_dates)

    sys.exit(0 if fail_count == 0 else 1)


if __name__ == "__main__":
    main()
