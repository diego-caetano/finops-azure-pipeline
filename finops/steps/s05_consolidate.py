"""
s05_consolidate.py — Consolidação de shares → allocation_share_final.

Combina shares de:
  - s03a: gold/dedicated_allocation_share  (Share=1.0 por recurso dedicado)
  - s03b: gold/aks_allocation_share_opencost (shares OpenCost por cluster)
  - s03c: gold/sql_pool_allocation_share    (shares SQL Pool por pesos)

Para ServiceKeys de r2s não cobertos pelos acima, distribui pelos pesos de
s04 (client_weights). Normaliza e valida que cada ServiceKey soma 1.0.
"""

import logging
import os
from typing import Optional

import pandas as pd

from finops.storage import pipeline_client
from finops.utils.dates import resolve_pipeline_date, to_partition

logger = logging.getLogger(__name__)

_R2S_PREFIX = os.getenv("RESOURCE_TO_SERVICE_PREFIX", "silver/resource_to_service")
_DEDICATED_OUT_PREFIX = os.getenv("DEDICATED_OUT_PREFIX", "gold/dedicated_allocation_share")
_AKS_OUT_PREFIX = os.getenv("AKS_OUT_PREFIX", "gold/aks_allocation_share_opencost")
_SQL_POOL_OUT_PREFIX = os.getenv("SQL_POOL_OUT_PREFIX", "gold/sql_pool_allocation_share")
_CLIENT_WEIGHTS_PREFIX = os.getenv("CLIENT_WEIGHTS_PREFIX", "silver/client_weights")
_FINAL_PREFIX = os.getenv("ALLOCATION_SHARE_FINAL_PREFIX", "gold/allocation_share_final")

_SHARE_TOLERANCE = 0.001
_OUTPUT_COLUMNS = ["Date", "ServiceKey", "Cliente", "Share", "DriverType", "AllocationMethod"]


def _read_csv_partition(
    store,
    prefix: str,
    date_str: str,
    include_prefix: str = "",
    exclude_patterns: list[str] | None = None,
) -> pd.DataFrame:
    """Read and concat CSVs under prefix/dt=date_str/, with optional filename filters."""
    blobs = store.list_blobs(f"{prefix}/{to_partition(date_str)}/")
    csv_blobs = [b for b in blobs if b.endswith(".csv")]
    if include_prefix:
        csv_blobs = [b for b in csv_blobs if include_prefix in b.split("/")[-1]]
    if exclude_patterns:
        csv_blobs = [
            b for b in csv_blobs
            if not any(pat in b.split("/")[-1] for pat in exclude_patterns)
        ]
    if not csv_blobs:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)
    frames = [store.read_csv(b) for b in csv_blobs]
    return pd.concat(frames, ignore_index=True)


def _normalize_shares(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize shares per ServiceKey to sum=1.0; warn on large deviations."""
    totals = df.groupby("ServiceKey")["Share"].transform("sum")
    deviating = df[(totals - 1.0).abs() > _SHARE_TOLERANCE]["ServiceKey"].unique()
    for key in deviating:
        s = float(df[df["ServiceKey"] == key]["Share"].sum())
        logger.warning("⚠ ServiceKey %s: soma=%.6f — normalizando", key, s)
    df = df.copy()
    df["Share"] = (df["Share"] / totals).round(6)
    return df


def run(pipeline_date: Optional[str] = None) -> None:
    date_str = resolve_pipeline_date(pipeline_date)
    logger.info("📅 s05_consolidate  date=%s", date_str)

    store = pipeline_client()
    partition = to_partition(date_str)

    # Load r2s for all ServiceKeys needing allocation
    r2s = store.read_parquet(f"{_R2S_PREFIX}/{partition}/resource_to_service.parquet")
    all_keys_df = r2s[["ServiceKey"]].drop_duplicates()
    logger.info("📊 %d ServiceKeys únicos em r2s", len(all_keys_df))

    # Load pre-computed shares from s03a, s03b, s03c
    # Note: each prefix may contain audit/detail CSVs — filter to allocation share files only
    df_dedicated = _read_csv_partition(store, _DEDICATED_OUT_PREFIX, date_str)
    df_aks = _read_csv_partition(
        store, _AKS_OUT_PREFIX, date_str,
        exclude_patterns=["namespaces_sem_alias_"],
    )
    df_sql = _read_csv_partition(
        store, _SQL_POOL_OUT_PREFIX, date_str,
        include_prefix="sqlpool_allocation_share_all_",
    )

    pre_frames = [df for df in (df_dedicated, df_aks, df_sql) if not df.empty]
    pre_computed = pd.concat(pre_frames, ignore_index=True) if pre_frames else pd.DataFrame(columns=_OUTPUT_COLUMNS)

    pre_computed_keys: set[str] = (
        set(pre_computed["ServiceKey"].unique()) if not pre_computed.empty else set()
    )
    logger.info(
        "📊 Pré-computados: %d keys (dedicated=%d, aks=%d, sql=%d)",
        len(pre_computed_keys), len(df_dedicated), len(df_aks), len(df_sql),
    )

    # Load client weights for uncovered ServiceKeys
    weights_df = store.read_csv(f"{_CLIENT_WEIGHTS_PREFIX}/{partition}/client_weights.csv")
    weights_df["Weight"] = pd.to_numeric(weights_df["Weight"], errors="coerce").fillna(0.0)
    w_total = weights_df["Weight"].sum()
    if w_total > 0:
        weights_df["Weight"] = (weights_df["Weight"] / w_total).round(6)

    # Build shares for uncovered ServiceKeys via cross join
    uncovered = all_keys_df[~all_keys_df["ServiceKey"].isin(pre_computed_keys)]
    logger.info("📊 ServiceKeys para client_weights: %d", len(uncovered))

    frames_out: list[pd.DataFrame] = []
    if not pre_computed.empty:
        frames_out.append(pre_computed[_OUTPUT_COLUMNS])

    if not uncovered.empty and not weights_df.empty:
        cross = uncovered.merge(
            weights_df[["Cliente", "Weight"]].rename(columns={"Weight": "Share"}),
            how="cross",
        )
        cross["Date"] = date_str
        cross["DriverType"] = "CLIENT_WEIGHT"
        cross["AllocationMethod"] = "WEIGHT_PROPORTIONAL"
        frames_out.append(cross[_OUTPUT_COLUMNS])

    if not frames_out:
        logger.warning("⚠ Nenhum share gerado — allocation_share_final não gravado")
        return

    df_final = pd.concat(frames_out, ignore_index=True)
    df_final["Share"] = pd.to_numeric(df_final["Share"], errors="coerce").fillna(0.0)
    df_final = _normalize_shares(df_final)

    logger.info(
        "✅ allocation_share_final: %d ServiceKeys, %d linhas",
        df_final["ServiceKey"].nunique(), len(df_final),
    )

    blob_name = f"{_FINAL_PREFIX}/{partition}/allocation_share_final.parquet"
    store.write_parquet(blob_name, df_final)
    logger.info("⬆️  Gravado: %s", blob_name)
