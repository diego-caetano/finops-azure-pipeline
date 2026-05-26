"""
s06_fact_allocated_cost.py — AllocatedCost = BilledCost × Share.

Cruza fact_cost × resource_to_service × allocation_share_final para gerar
a tabela final de custos alocados por cliente.

Saídas (gold/fact_allocated_cost/dt=<date>/):
  fact_allocated_cost_<date>.parquet   — linhas alocadas com detalhe completo
  final_cost_by_client_<date>.parquet  — resumo por cliente (DedicatedCost, SharedAllocatedCost, TotalAllocatedCost, ShareOfTotal)
  final_cost_detailed_<date>.parquet   — detalhe por cliente × ServiceKey
  reconciliation_<date>.csv            — TotalFactCost vs TotalAllocated vs TotalUnallocated
  unallocated_cost_<date>.csv          — recursos não alocados (NO_SERVICEKEY | HAS_SERVICEKEY_NO_SHARE | SHARE_IS_ZERO)
"""

import gc
import logging
import os
from typing import Optional

import pandas as pd

from finops.storage import pipeline_client, upload_csv, upload_parquet
from finops.utils.dates import resolve_pipeline_date, to_partition

logger = logging.getLogger(__name__)

_FACT_COST_PREFIX = os.getenv("FACT_COST_PREFIX", "silver/fact_cost")
_R2S_PREFIX = os.getenv("RESOURCE_TO_SERVICE_PREFIX", "silver/resource_to_service")
_FINAL_PREFIX = os.getenv("ALLOCATION_SHARE_FINAL_PREFIX", "gold/allocation_share_final")
_OUT_PREFIX = os.getenv("FACT_ALLOCATED_COST_PREFIX", "gold/fact_allocated_cost")

_DEDICATED_METHOD = "DEDICATED_TAG"

_FACT_COLUMNS = [
    "Date", "ResourceId", "ResourceName", "ResourceGroupName", "ResourceType",
    "SubscriptionId", "ServiceName", "ServiceKey", "Cliente",
    "Cost", "Share", "AllocatedCost", "AllocationMethod", "DriverType",
]
_BY_CLIENT_COLUMNS = [
    "Date", "Cliente", "DedicatedCost", "SharedAllocatedCost", "TotalAllocatedCost", "ShareOfTotal",
]
_DETAILED_COLUMNS = ["Date", "Cliente", "ServiceKey", "AllocatedCost", "AllocationMethod"]
_RECONCILIATION_COLUMNS = [
    "Date", "TotalFactCost", "TotalAllocated", "TotalUnallocated",
    "AllocatedPct", "UnallocatedPct", "Difference",
]
_UNALLOCATED_COLUMNS = [
    "Date", "ResourceId", "ResourceName", "ResourceGroupName", "ServiceKey", "Cost", "Reason",
]


def _rss_mb() -> float:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except Exception:
        pass
    return 0.0


def run(pipeline_date: Optional[str] = None) -> None:
    date_str = resolve_pipeline_date(pipeline_date)
    logger.info("📅 s06_fact_allocated_cost  date=%s", date_str)

    store = pipeline_client()
    partition = to_partition(date_str)
    storage_account = os.environ["PIPELINE_STORAGE_ACCOUNT"]
    container = os.environ["PIPELINE_CONTAINER"]
    out_dir = f"{_OUT_PREFIX}/{partition}"

    logger.info("📊 s06 RSS antes de carregar dados: %.0fMB", _rss_mb())

    # ── Load inputs ────────────────────────────────────────────────────────
    df_fc = store.read_parquet(f"{_FACT_COST_PREFIX}/{partition}/fact_cost.parquet")
    logger.info("📊 df_fc: %d linhas, RSS=%.0fMB", len(df_fc), _rss_mb())

    df_r2s = store.read_parquet(f"{_R2S_PREFIX}/{partition}/resource_to_service.parquet")
    logger.info("📊 df_r2s: %d linhas, RSS=%.0fMB", len(df_r2s), _rss_mb())

    df_shares = store.read_parquet(f"{_FINAL_PREFIX}/{partition}/allocation_share_final.parquet")
    logger.info("📊 df_shares: %d linhas, RSS=%.0fMB", len(df_shares), _rss_mb())

    total_billed = float(df_fc["Cost"].sum())
    logger.info("💰 TotalFactCost=%.6f", total_billed)

    # ── Join 1: fact_cost → r2s (ServiceKey por ResourceId) ───────────────
    # Dedup r2s: um ResourceId → um ServiceKey (s02 é determinístico)
    r2s_key = df_r2s[["ResourceId", "ServiceKey"]].drop_duplicates(subset=["ResourceId"])
    del df_r2s
    gc.collect()

    merged = df_fc.merge(r2s_key, on="ResourceId", how="left")
    del df_fc, r2s_key
    gc.collect()
    logger.info("📊 após join r2s: %d linhas, RSS=%.0fMB", len(merged), _rss_mb())

    # Separa recursos sem ServiceKey → NO_SERVICEKEY
    mask_no_sk = merged["ServiceKey"].isna()
    unalloc_no_sk = merged.loc[
        mask_no_sk, ["ResourceId", "ResourceName", "ResourceGroupName", "Cost"]
    ].copy()
    unalloc_no_sk["ServiceKey"] = ""
    unalloc_no_sk["Reason"] = "NO_SERVICEKEY"

    merged_with_sk = merged[~mask_no_sk].copy()
    del merged
    gc.collect()

    # ── Otimização de memória antes do join de fan-out ─────────────────────
    for col in ("ServiceKey", "Cliente", "AllocationMethod"):
        if col in df_shares.columns:
            df_shares[col] = df_shares[col].astype("category")
    if "DriverType" in df_shares.columns:
        df_shares["DriverType"] = df_shares["DriverType"].astype("category")

    # ── Join 2: → allocation_share_final (Cliente, Share, AllocationMethod) ─
    share_cols = ["ServiceKey", "Cliente", "Share", "AllocationMethod"]
    if "DriverType" in df_shares.columns:
        share_cols.append("DriverType")

    logger.info("📊 iniciando join shares (%d × %d), RSS=%.0fMB",
                len(merged_with_sk), len(df_shares), _rss_mb())
    merged2 = merged_with_sk.merge(df_shares[share_cols], on="ServiceKey", how="left")
    del merged_with_sk, df_shares
    gc.collect()
    logger.info("📊 após join shares: %d linhas, RSS=%.0fMB", len(merged2), _rss_mb())

    if "DriverType" not in merged2.columns:
        merged2["DriverType"] = ""

    # Separa recursos com ServiceKey mas sem Share → HAS_SERVICEKEY_NO_SHARE
    mask_no_share = merged2["Cliente"].isna()
    unalloc_no_share = merged2.loc[
        mask_no_share, ["ResourceId", "ResourceName", "ResourceGroupName", "ServiceKey", "Cost"]
    ].copy()
    unalloc_no_share["Reason"] = "HAS_SERVICEKEY_NO_SHARE"

    merged_covered = merged2[~mask_no_share].copy()
    del merged2
    gc.collect()

    # ── Calcula AllocatedCost = Cost × Share ──────────────────────────────
    merged_covered["AllocatedCost"] = (
        pd.to_numeric(merged_covered["Cost"], errors="coerce").fillna(0.0) *
        pd.to_numeric(merged_covered["Share"], errors="coerce").fillna(0.0)
    ).round(6)
    merged_covered["Date"] = date_str

    # Separa linhas com Share=0 → SHARE_IS_ZERO (recurso coberto por outros clientes)
    share_num = pd.to_numeric(merged_covered["Share"], errors="coerce").fillna(0.0)
    mask_zero_share = share_num == 0.0
    unalloc_zero = merged_covered.loc[
        mask_zero_share, ["ResourceId", "ResourceName", "ResourceGroupName", "ServiceKey"]
    ].copy()
    unalloc_zero["Cost"] = 0.0  # audit marker: custo coberto por outros clientes
    unalloc_zero["Reason"] = "SHARE_IS_ZERO"

    df_fact_rows = merged_covered[~mask_zero_share].copy()
    del merged_covered
    gc.collect()

    # Drop rows with NaN AllocationMethod (duplicatas de fan-out SQLPOOL)
    null_alloc = df_fact_rows["AllocationMethod"].isna()
    if null_alloc.any():
        n = int(null_alloc.sum())
        cost_lost = float(df_fact_rows.loc[null_alloc, "AllocatedCost"].sum())
        logger.warning(
            "⚠ Removendo %d linhas sem AllocationMethod (duplicatas SQLPOOL) | AllocatedCost=%.2f",
            n, cost_lost,
        )
        df_fact_rows = df_fact_rows[~null_alloc].copy()
        gc.collect()

    # ── Output 1: fact_allocated_cost_{date}.parquet ──────────────────────
    df_out = df_fact_rows[_FACT_COLUMNS].reset_index(drop=True)
    total_allocated = float(df_out["AllocatedCost"].sum())
    del df_fact_rows
    gc.collect()

    blob1 = f"{out_dir}/fact_allocated_cost_{date_str}.parquet"
    upload_parquet(df_out, storage_account, container, blob1)
    logger.info("✅ fact_allocated_cost: %d linhas → %s", len(df_out), blob1)

    # ── Output 2: final_cost_by_client_{date}.parquet ─────────────────────
    ded = (
        df_out.loc[df_out["AllocationMethod"].str.startswith("DEDICATED", na=False)]
        .groupby("Cliente")["AllocatedCost"].sum()
    )
    tot = df_out.groupby("Cliente")["AllocatedCost"].sum()
    shared = tot.subtract(ded, fill_value=0.0)

    df_by_client = pd.DataFrame({
        "Date": date_str,
        "Cliente": tot.index,
        "DedicatedCost": ded.reindex(tot.index, fill_value=0.0).round(6).values,
        "SharedAllocatedCost": shared.round(6).values,
        "TotalAllocatedCost": tot.round(6).values,
        "ShareOfTotal": (
            (tot / total_allocated * 100.0).round(6).values if total_allocated else 0.0
        ),
    })[_BY_CLIENT_COLUMNS].sort_values("TotalAllocatedCost", ascending=False).reset_index(drop=True)

    blob2 = f"{out_dir}/final_cost_by_client_{date_str}.parquet"
    upload_parquet(df_by_client, storage_account, container, blob2)
    logger.info("✅ final_cost_by_client: %d clientes → %s", len(df_by_client), blob2)

    for _, r in df_by_client.iterrows():
        logger.info("  %-30s  ded=%.2f  shared=%.2f  total=%.2f  (%.1f%%)",
                    r["Cliente"], r["DedicatedCost"], r["SharedAllocatedCost"],
                    r["TotalAllocatedCost"], r["ShareOfTotal"])

    # ── Output 3: final_cost_detailed_{date}.parquet ──────────────────────
    df_detailed = (
        df_out.groupby(["Cliente", "ServiceKey", "AllocationMethod"])["AllocatedCost"]
        .sum().round(6).reset_index()
    )
    df_detailed["Date"] = date_str
    df_detailed = (
        df_detailed[_DETAILED_COLUMNS]
        .sort_values(["Cliente", "AllocatedCost"], ascending=[True, False])
        .reset_index(drop=True)
    )
    del df_out
    gc.collect()

    blob3 = f"{out_dir}/final_cost_detailed_{date_str}.parquet"
    upload_parquet(df_detailed, storage_account, container, blob3)
    logger.info("✅ final_cost_detailed: %d linhas → %s", len(df_detailed), blob3)
    del df_detailed
    gc.collect()

    # ── Reconciliação ─────────────────────────────────────────────────────
    total_unallocated = (
        float(unalloc_no_sk["Cost"].sum()) +
        float(unalloc_no_share["Cost"].sum() if not unalloc_no_share.empty else 0.0)
        # unalloc_zero não soma (Cost=0, recurso coberto por outros clientes)
    )
    difference = round(total_billed - total_allocated - total_unallocated, 6)
    alloc_pct = round(100.0 * total_allocated / total_billed, 6) if total_billed else 0.0
    unalloc_pct = round(100.0 * total_unallocated / total_billed, 6) if total_billed else 0.0

    logger.info(
        "💰 TotalFactCost=%.6f | TotalAllocated=%.6f (%.1f%%) | TotalUnallocated=%.6f (%.1f%%) | Diff=%.6f",
        total_billed, total_allocated, alloc_pct, total_unallocated, unalloc_pct, difference,
    )
    # Tolerância de 0.05 cobre arredondamento de ponto flutuante nas multiplicações
    # Cost × Share (normal para bases grandes). Erros reais são na casa de centenas.
    if abs(difference) > 0.05:
        raise ValueError(
            f"Reconciliation falhou: difference={difference:.6f} "
            f"(total_billed={total_billed:.6f}, allocated={total_allocated:.6f}, "
            f"unallocated={total_unallocated:.6f})"
        )

    # ── Output 4: reconciliation_{date}.csv ──────────────────────────────
    df_recon = pd.DataFrame([{
        "Date": date_str,
        "TotalFactCost": round(total_billed, 6),
        "TotalAllocated": round(total_allocated, 6),
        "TotalUnallocated": round(total_unallocated, 6),
        "AllocatedPct": alloc_pct,
        "UnallocatedPct": unalloc_pct,
        "Difference": difference,
    }], columns=_RECONCILIATION_COLUMNS)

    blob4 = f"{out_dir}/reconciliation_{date_str}.csv"
    upload_csv(df_recon, storage_account, container, blob4)
    logger.info("✅ reconciliation → %s", blob4)

    # ── Output 5: unallocated_cost_{date}.csv ─────────────────────────────
    unalloc_frames = []
    for df_u, has_date_col in (
        (unalloc_no_sk, False),
        (unalloc_no_share, False),
        (unalloc_zero, False),
    ):
        if df_u.empty:
            continue
        df_u = df_u.copy()
        df_u["Date"] = date_str
        for col in _UNALLOCATED_COLUMNS:
            if col not in df_u.columns:
                df_u[col] = ""
        unalloc_frames.append(df_u[_UNALLOCATED_COLUMNS])

    if unalloc_frames:
        df_unalloc = pd.concat(unalloc_frames, ignore_index=True)
    else:
        df_unalloc = pd.DataFrame(columns=_UNALLOCATED_COLUMNS)

    blob5 = f"{out_dir}/unallocated_cost_{date_str}.csv"
    upload_csv(df_unalloc, storage_account, container, blob5)
    logger.info("✅ unallocated_cost: %d linhas → %s", len(df_unalloc), blob5)

    logger.info(
        "✅ s06 concluído | alocado=%.2f (%.1f%%) | não-alocado=%.2f (%.1f%%)",
        total_allocated, alloc_pct, total_unallocated, unalloc_pct,
    )
