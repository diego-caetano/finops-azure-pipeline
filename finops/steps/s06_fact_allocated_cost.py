"""
s06_fact_allocated_cost.py — AllocatedCost = BilledCost × Share.

Cruza fact_cost × resource_to_service × allocation_share_final para gerar
a tabela final de custos alocados por cliente.

Join:
  fact_cost[ResourceId, Cost]
  → r2s[ResourceId → ServiceKey]
  → allocation_share_final[ServiceKey → (Cliente, Share, AllocationMethod)]
  → AllocatedCost = Cost × Share
"""

import gc
import logging
import os
from typing import Optional

import pandas as pd


def _rss_mb() -> float:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except Exception:
        pass
    return 0.0

from finops.storage import pipeline_client
from finops.utils.dates import resolve_pipeline_date, to_partition

logger = logging.getLogger(__name__)

_FACT_COST_PREFIX = os.getenv("FACT_COST_PREFIX", "silver/fact_cost")
_R2S_PREFIX = os.getenv("RESOURCE_TO_SERVICE_PREFIX", "silver/resource_to_service")
_FINAL_PREFIX = os.getenv("ALLOCATION_SHARE_FINAL_PREFIX", "gold/allocation_share_final")
_OUT_PREFIX = os.getenv("FACT_ALLOCATED_COST_PREFIX", "gold/fact_allocated_cost")

_OUTPUT_COLUMNS = [
    "Date", "ResourceId", "ServiceKey", "Cliente",
    "Cost", "Share", "AllocatedCost", "AllocationMethod",
]


def run(pipeline_date: Optional[str] = None) -> None:
    date_str = resolve_pipeline_date(pipeline_date)
    logger.info("📅 s06_fact_allocated_cost  date=%s", date_str)

    store = pipeline_client()
    partition = to_partition(date_str)

    logger.info("📊 s06 RSS antes de carregar dados: %.0fMB", _rss_mb())

    df_fc = store.read_parquet(f"{_FACT_COST_PREFIX}/{partition}/fact_cost.parquet")
    logger.info("📊 df_fc carregado: %d linhas, dtypes=%s, RSS=%.0fMB",
                len(df_fc), dict(df_fc.dtypes), _rss_mb())

    df_r2s = store.read_parquet(f"{_R2S_PREFIX}/{partition}/resource_to_service.parquet")
    logger.info("📊 df_r2s carregado: %d linhas, mem=%.0fMB, RSS=%.0fMB",
                len(df_r2s), df_r2s.memory_usage(deep=True).sum() / 1e6, _rss_mb())

    df_shares = store.read_parquet(f"{_FINAL_PREFIX}/{partition}/allocation_share_final.parquet")
    logger.info("📊 df_shares carregado: %d linhas, mem=%.0fMB, dtypes=%s, RSS=%.0fMB",
                len(df_shares), df_shares.memory_usage(deep=True).sum() / 1e6,
                dict(df_shares.dtypes), _rss_mb())

    # Cast high-cardinality string columns to category to reduce memory footprint
    # before the large fan-out merge (N resources × M clients).
    for col in ("ServiceKey", "Cliente", "AllocationMethod"):
        df_shares[col] = df_shares[col].astype("category")
    logger.info("📊 df_shares após category: mem=%.0fMB, RSS=%.0fMB",
                df_shares.memory_usage(deep=True).sum() / 1e6, _rss_mb())

    # fact_cost → r2s (get ServiceKey per ResourceId)
    # Deduplicate r2s: each ResourceId maps to exactly one ServiceKey (deterministic
    # in s02 — same resource always follows the same rule regardless of charge type).
    # Without dedup, fact_cost has multiple charge rows per ResourceId AND r2s mirrors
    # those rows 1:1, producing a many-to-many join explosion (N² per unique ResourceId).
    total_billed = float(df_fc["Cost"].sum())
    r2s_key = df_r2s[["ResourceId", "ServiceKey"]].drop_duplicates(subset=["ResourceId"])
    del df_r2s

    merged = df_fc[["ResourceId", "Cost"]].merge(r2s_key, on="ResourceId", how="left")
    del df_fc, r2s_key
    gc.collect()
    logger.info("📊 merged após 1º join: %d linhas, RSS=%.0fMB", len(merged), _rss_mb())

    # → allocation_share_final (get Cliente, Share, AllocationMethod per ServiceKey)
    logger.info("📊 iniciando 2º merge (merged=%d × shares=%d)...", len(merged), len(df_shares))
    merged = merged.merge(
        df_shares[["ServiceKey", "Cliente", "Share", "AllocationMethod"]],
        on="ServiceKey",
        how="left",
    )
    logger.info("📊 merged após 2º join: %d linhas, RSS=%.0fMB", len(merged), _rss_mb())
    del df_shares
    gc.collect()
    logger.info("📊 após del df_shares + gc: RSS=%.0fMB", _rss_mb())

    n_unmatched = merged["Cliente"].isna().sum()
    if n_unmatched:
        logger.warning(
            "⚠ %d linhas sem alocação (ServiceKey ausente em allocation_share_final)", n_unmatched
        )

    merged = merged.dropna(subset=["Cliente"]).copy()
    merged["AllocatedCost"] = pd.to_numeric(merged["Cost"], errors="coerce").fillna(0.0) * \
                               pd.to_numeric(merged["Share"], errors="coerce").fillna(0.0)
    merged["Date"] = date_str

    df_out = merged[_OUTPUT_COLUMNS].reset_index(drop=True)

    total_allocated = float(df_out["AllocatedCost"].sum())
    logger.info("💰 Custo total faturado=%.2f | Total alocado=%.2f", total_billed, total_allocated)

    by_client = df_out.groupby("Cliente")["AllocatedCost"].sum().sort_values(ascending=False)
    for cliente, val in by_client.items():
        pct = 100.0 * val / total_billed if total_billed else 0.0
        logger.info("  %-30s  %.2f  (%.1f%%)", cliente, val, pct)

    blob_name = f"{_OUT_PREFIX}/{partition}/fact_allocated_cost.parquet"
    store.write_parquet(blob_name, df_out)
    logger.info("✅ fact_allocated_cost: %d linhas → %s", len(df_out), blob_name)
