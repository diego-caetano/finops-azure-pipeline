"""
s01_ingest_focus.py — Ingestão do FOCUS Report → fact_cost

Lê o parquet do FOCUS Report para a data alvo, filtra por ChargePeriodStart,
mapeia colunas para o schema do fact_cost e grava em silver/fact_cost/dt=.

Lógica de seleção de partição:
  1. Lista blobs sob FOCUS_PREFIX e extrai partições yyyyMMdd-yyyyMMdd
  2. Ordena pelas mais recentes (até _MAX_PARTITIONS para trás)
  3. Seleciona a primeira cujo range de datas contém a data alvo
  4. Lê o parquet, filtra por ChargePeriodStart == data_alvo
"""

import io
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq

from finops.storage import focus_client, pipeline_client
from finops.utils.dates import resolve_pipeline_date, to_partition

logger = logging.getLogger(__name__)

_FOCUS_PREFIX = os.getenv("FOCUS_PREFIX", "costexport/azure-cost-export-focus10-52548376")
_FACT_COST_PREFIX = os.getenv("FACT_COST_PREFIX", "silver/fact_cost")
_MAX_PARTITIONS = 6

# Colunas FOCUS → fact_cost. Inclui alternativas para variações do export Azure.
_COLUMN_CANDIDATES: dict[str, list[str]] = {
    "ResourceId":        ["ResourceId"],
    "ResourceName":      ["ResourceName"],
    "ResourceGroupName": ["ResourceGroupName", "x_ResourceGroupName"],
    "ResourceType":      ["ResourceType"],
    "SubscriptionId":    ["SubAccountId", "SubscriptionId", "x_SubscriptionId"],
    "ServiceName":       ["ServiceName"],
    "Cost":              ["BilledCost"],
    "Tags":              ["Tags", "tags"],
}

_FACT_COST_COLUMNS = [
    "Date", "ResourceId", "ResourceName", "ResourceGroupName",
    "ResourceType", "SubscriptionId", "ServiceName", "Cost", "Tags",
]


# ---------------------------------------------------------------------------
# Partition helpers
# ---------------------------------------------------------------------------

def _parse_partition_range(path: str) -> Optional[tuple[datetime, datetime]]:
    """Extract (start, end) from a path containing yyyyMMdd-yyyyMMdd segment."""
    m = re.search(r"(\d{8})-(\d{8})", path)
    if not m:
        return None
    try:
        start = datetime.strptime(m.group(1), "%Y%m%d")
        end = datetime.strptime(m.group(2), "%Y%m%d")
        return start, end
    except ValueError:
        return None


def _find_partition_blobs(focus_store, target_date: str) -> list[str]:
    """Return blob paths in the best partition for target_date, sorted by name."""
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")

    all_blobs = focus_store.list_blobs(_FOCUS_PREFIX)
    parquet_blobs = [b for b in all_blobs if b.endswith(".parquet")]

    # Group blobs by their partition prefix
    partitions: dict[str, list[str]] = {}
    for blob in parquet_blobs:
        rng = _parse_partition_range(blob)
        if rng is None:
            continue
        start, end = rng
        key = f"{start.strftime('%Y%m%d')}-{(end + timedelta(days=0)).strftime('%Y%m%d')}"
        partitions.setdefault(key, []).append(blob)

    # Sort partitions by start date descending (most recent first)
    sorted_keys = sorted(
        partitions.keys(),
        key=lambda k: datetime.strptime(k[:8], "%Y%m%d"),
        reverse=True,
    )

    for key in sorted_keys[:_MAX_PARTITIONS]:
        start = datetime.strptime(key[:8], "%Y%m%d")
        end = datetime.strptime(key[9:], "%Y%m%d")
        # Include target_date if it falls within [start, end] (inclusive)
        if start <= target_dt <= end:
            blobs = sorted(partitions[key])
            logger.info("📂 Partição selecionada: %s (%d arquivo(s))", key, len(blobs))
            return blobs

    raise FileNotFoundError(
        f"Nenhuma partição encontrada contendo {target_date} "
        f"(verificadas {min(len(sorted_keys), _MAX_PARTITIONS)} partições)"
    )


# ---------------------------------------------------------------------------
# Column resolution
# ---------------------------------------------------------------------------

def _resolve_column(df_cols: list[str], candidates: list[str], field: str) -> Optional[str]:
    """Return the first candidate column present in df_cols, or None."""
    cols_lower = {c.lower(): c for c in df_cols}
    for candidate in candidates:
        actual = cols_lower.get(candidate.lower())
        if actual is not None:
            return actual
    logger.warning("⚠ Coluna '%s' não encontrada. Candidatas: %s. Disponíveis: %s",
                   field, candidates, df_cols[:20])
    return None


def _extract_rg_from_resource_id(resource_id: str) -> str:
    """Extract resource group name from Azure Resource ID, or return empty string."""
    m = re.search(r"/resourceGroups/([^/]+)/", str(resource_id), re.IGNORECASE)
    return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# Step entry point
# ---------------------------------------------------------------------------

def run(pipeline_date: Optional[str] = None) -> None:
    date_str = resolve_pipeline_date(pipeline_date)
    logger.info("📅 s01_ingest_focus  date=%s", date_str)

    focus_store = focus_client()
    pipe_store = pipeline_client()

    # 1. Locate parquet file(s) for target date
    blob_paths = _find_partition_blobs(focus_store, date_str)

    # 2. Read and concatenate (usually a single file per partition)
    frames: list[pd.DataFrame] = []
    for blob_path in blob_paths:
        raw = focus_store.download_bytes(blob_path)
        table = pq.read_table(io.BytesIO(raw))
        df_raw = table.to_pandas()

        # 3. Filter rows for the target date
        cols = list(df_raw.columns)
        date_col = _resolve_column(cols, ["ChargePeriodStart"], "ChargePeriodStart")
        if date_col is None:
            raise ValueError("Coluna ChargePeriodStart não encontrada no FOCUS parquet")

        df_raw[date_col] = pd.to_datetime(df_raw[date_col], utc=True)
        mask = df_raw[date_col].dt.strftime("%Y-%m-%d") == date_str
        df_day = df_raw[mask].copy()

        logger.info("📊 %s: %d linhas totais → %d para %s",
                    blob_path.split("/")[-2], len(df_raw), len(df_day), date_str)

        if df_day.empty:
            continue
        frames.append(df_day)

    if not frames:
        logger.warning("⚠ Nenhuma linha encontrada para %s — fact_cost não gravado", date_str)
        return

    df = pd.concat(frames, ignore_index=True)
    cols = list(df.columns)

    # 4. Map columns to fact_cost schema
    mapping: dict[str, str] = {}
    for target_field, candidates in _COLUMN_CANDIDATES.items():
        src = _resolve_column(cols, candidates, target_field)
        if src:
            mapping[src] = target_field

    df_out = df.rename(columns=mapping)

    # Add Date column from ChargePeriodStart (already filtered = target date)
    df_out["Date"] = date_str

    # Fallback: extract ResourceGroupName from ResourceId if column missing
    if "ResourceGroupName" not in df_out.columns and "ResourceId" in df_out.columns:
        logger.warning("⚠ ResourceGroupName ausente — extraindo do ResourceId")
        df_out["ResourceGroupName"] = df_out["ResourceId"].apply(_extract_rg_from_resource_id)

    # 5. Keep only fact_cost columns that exist
    available = [c for c in _FACT_COST_COLUMNS if c in df_out.columns]
    missing = [c for c in _FACT_COST_COLUMNS if c not in df_out.columns]
    if missing:
        logger.warning("⚠ Colunas ausentes no output: %s", missing)

    df_out = df_out[available].copy()
    df_out["Cost"] = pd.to_numeric(df_out["Cost"], errors="coerce").fillna(0.0)
    df_out = df_out[df_out["Cost"] != 0].reset_index(drop=True)

    logger.info("✅ fact_cost: %d linhas, custo total=%.2f", len(df_out), df_out["Cost"].sum())

    # 6. Write output
    blob_name = f"{_FACT_COST_PREFIX}/{to_partition(date_str)}/fact_cost.parquet"
    pipe_store.write_parquet(blob_name, df_out)
    logger.info("⬆️  Gravado: %s", blob_name)
