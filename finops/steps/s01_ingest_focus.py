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

# Subscriptions de faturamento cujos recursos devem ser incluídos no pipeline.
# Separadas por vírgula. Vazio = sem filtro (não recomendado em produção).
_BILLING_SUBSCRIPTION_IDS: frozenset[str] = frozenset(
    s.strip().lower()
    for s in os.getenv(
        "BILLING_SUBSCRIPTION_IDS",
        "52d4423b-7ed9-4673-b8e2-fa21cdb83176,3f6d197f-f70b-4c2c-b981-8bb575d47a7a,"
        "7038ef68-75ba-48fd-955a-4e67b86bca45,66dc1f31-e830-409e-afe8-621c75ff92fc,"
        "b9fa758a-0ccf-48a6-9033-7e92c14091e7,80d65916-3cc3-4fab-a902-c2832d851fdf,"
        "0d606793-89c9-44a2-a1f9-a30fc650a9cd,640f2402-d9d1-4755-a8f6-8930cbb132e8",
    ).split(",")
    if s.strip()
)

# Colunas FOCUS → fact_cost. Inclui alternativas para variações do export Azure.
_COLUMN_CANDIDATES: dict[str, list[str]] = {
    "ResourceId":        ["ResourceId"],
    "ResourceName":      ["ResourceName"],
    "ResourceGroupName": ["ResourceGroupName", "x_ResourceGroupName"],
    "ResourceType":      ["ResourceType"],
    "SubscriptionId":    ["SubAccountId", "SubscriptionId", "x_SubscriptionId"],
    "ServiceName":       ["ServiceName"],
    "Cost":              ["EffectiveCost"],
    "Tags":              ["Tags", "tags"],
}

_FACT_COST_COLUMNS = [
    "Date", "ResourceId", "ResourceName", "ResourceGroupName",
    "ResourceType", "SubscriptionId", "ServiceName", "Cost", "Tags",
]

# Reserva ociosa: horas de reserva não usadas vêm no FOCUS como Usage com
# CommitmentDiscountStatus=Unused e SEM subscription/ResourceId — o filtro de
# subscriptions as descartaria, e o pipeline ficaria abaixo do custo amortizado do
# portal (billing profile). Elas entram com um ResourceType sintético que o s02
# mapeia para ServiceKey=RESERVA_OCIOSA (rateio proporcional em s05).
UNUSED_RI_RESOURCE_TYPE = "microsoft.capacity/reservationorders/reservations"


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


def _unused_reservation_mask(df: pd.DataFrame, in_scope: pd.Series) -> pd.Series:
    """Linhas de reserva ociosa pertencentes aos billing profiles das subscriptions do escopo.

    A linha Unused não traz subscription; o vínculo com o escopo é o billing profile
    (o mesmo que contém as subscriptions filtradas). Reservas de outros profiles
    do mesmo export (ex.: LICENCA DE SOFTWARE) ficam de fora.
    """
    status_col = _resolve_column(list(df.columns), ["CommitmentDiscountStatus"], "CommitmentDiscountStatus")
    if status_col is None:
        return pd.Series(False, index=df.index)
    mask = df[status_col].astype(str).str.lower().eq("unused")

    profile_col = _resolve_column(list(df.columns), ["x_BillingProfileId"], "x_BillingProfileId")
    if profile_col is not None:
        scope_profiles = set(df.loc[in_scope, profile_col].dropna().astype(str))
        mask &= df[profile_col].astype(str).isin(scope_profiles)
    return mask & ~in_scope


def _label_unused_reservations(df: pd.DataFrame, mask: pd.Series) -> None:
    """Preenche identificadores sintéticos nas linhas de reserva ociosa (in place)."""
    cols = list(df.columns)
    id_col = _resolve_column(cols, ["CommitmentDiscountId"], "CommitmentDiscountId")
    name_col = _resolve_column(cols, ["CommitmentDiscountName"], "CommitmentDiscountName")
    ri_id = df.loc[mask, id_col].astype(str).str.lower() if id_col else "reserva-ociosa"
    df.loc[mask, "ResourceId"] = ri_id
    df.loc[mask, "ResourceName"] = df.loc[mask, name_col].astype(str) if name_col else ri_id
    df.loc[mask, "ResourceGroupName"] = ""
    df.loc[mask, "ResourceType"] = UNUSED_RI_RESOURCE_TYPE
    df.loc[mask, "SubscriptionId"] = ""


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

    # Usa EffectiveCost (custo amortizado) em vez de BilledCost.
    # BilledCost=0 nas linhas de Usage cobertas por RI — o custo real está em EffectiveCost,
    # distribuído de volta para os recursos com ResourceGroup correto (incluindo MC_* dos clusters AKS).
    # Purchase lines (Reservation order sem RG) ficam com EffectiveCost=0 e são removidas abaixo.
    if "BilledCost" in df.columns and "EffectiveCost" in df.columns:
        billed_total    = df["BilledCost"].sum()
        effective_total = df["EffectiveCost"].sum()
        logger.info(
            "💰 BilledCost total: %.2f | EffectiveCost total: %.2f | "
            "Diferença (RI amortizada): %.2f",
            billed_total, effective_total, effective_total - billed_total,
        )

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

    # 5. Filtro de subscriptions — mantém apenas as subscriptions alvo do pipeline
    # FOCUS armazena SubscriptionId como "/subscriptions/{guid}" — strip do prefixo antes de comparar
    if _BILLING_SUBSCRIPTION_IDS and "SubscriptionId" in df_out.columns:
        before = len(df_out)
        sub_ids_normalized = (
            df_out["SubscriptionId"]
            .str.lower()
            .str.replace(r"^.*/subscriptions/", "", regex=True)
        )
        in_scope = sub_ids_normalized.isin(_BILLING_SUBSCRIPTION_IDS)
        unused_ri = _unused_reservation_mask(df_out, in_scope)
        if unused_ri.any():
            _label_unused_reservations(df_out, unused_ri)
            logger.info(
                "📋 Reserva ociosa: %d linhas, custo=%.2f → ServiceKey RESERVA_OCIOSA",
                int(unused_ri.sum()),
                pd.to_numeric(df_out.loc[unused_ri, "Cost"], errors="coerce").sum(),
            )
        df_out = df_out[in_scope | unused_ri].copy()
        dropped = before - len(df_out)
        logger.info(
            "📋 Filtro subscriptions: %d → %d linhas (%d removidas de outras subscriptions)",
            before, len(df_out), dropped,
        )
    elif "SubscriptionId" not in df_out.columns:
        logger.warning("⚠ Coluna SubscriptionId ausente — filtro de subscription não aplicado")

    # 6. Keep only fact_cost columns that exist
    available = [c for c in _FACT_COST_COLUMNS if c in df_out.columns]
    missing = [c for c in _FACT_COST_COLUMNS if c not in df_out.columns]
    if missing:
        logger.warning("⚠ Colunas ausentes no output: %s", missing)

    df_out = df_out[available].copy()
    df_out["Cost"] = pd.to_numeric(df_out["Cost"], errors="coerce").fillna(0.0)
    n_before = len(df_out)
    df_out = df_out[df_out["Cost"] > 0].reset_index(drop=True)
    n_zero = n_before - len(df_out)
    if n_zero:
        logger.info(
            "🔍 Filtro EffectiveCost>0: removidas %d linhas (%.1f%%) "
            "— Purchase/RI sem consumo",
            n_zero, n_zero / n_before * 100,
        )

    logger.info("✅ fact_cost: %d linhas, custo total=%.2f", len(df_out), df_out["Cost"].sum())

    # 7. Write output
    blob_name = f"{_FACT_COST_PREFIX}/{to_partition(date_str)}/fact_cost.parquet"
    pipe_store.write_parquet(blob_name, df_out)
    logger.info("⬆️  Gravado: %s", blob_name)
