"""
s02_classify.py — Classificação de recursos → resource_to_service

Aplica hierarquia em 6 regras para mapear cada linha do fact_cost a um
ServiceKey, ServiceType e EffectiveFinopsCliente.

Hierarquia:
  1. TAG_RESOURCE    (Confidence=100) — tag FINOPS-CLIENTE no recurso
  2. TAG_RG_INHERITED (Confidence=95) — tag FINOPS-CLIENTE no RG ≠ COMPARTILHADO
  3. TAG_RG_SHARED   (Confidence=90) — tag no RG = COMPARTILHADO
  4. OVERRIDE_RG_AUTO (Confidence=95) — RG padrão MC_* → AKS cluster
  5. RG_INFERRED     (Confidence=70) — RG sem tag → compartilhado por inferência
  6. UNKNOWN         (Confidence=0)  — sem classificação
"""

import json
import logging
import os
import re
from typing import Optional

import numpy as np
import pandas as pd

from finops.storage import pipeline_client
from finops.utils.dates import resolve_pipeline_date, to_partition

logger = logging.getLogger(__name__)

_REFERENCE_PREFIX = os.getenv("REFERENCE_PREFIX", "config/reference")
_FACT_COST_PREFIX = os.getenv("FACT_COST_PREFIX", "silver/fact_cost")
_R2S_PREFIX = os.getenv("RESOURCE_TO_SERVICE_PREFIX", "silver/resource_to_service")

_OUTPUT_COLUMNS = [
    "Date", "ResourceId", "ResourceType", "ResourceName", "ResourceGroupName",
    "ServiceKey", "ServiceType", "Rule", "Confidence",
    "EffectiveFinopsCliente", "FinopsClienteSource", "Notes",
]

_TAG_KEY = "FINOPS-CLIENTE"
_COMPARTILHADO = "COMPARTILHADO"


def _extract_finops_tag(tags_raw) -> Optional[str]:
    """Extract FINOPS-CLIENTE value from Tags column (JSON string, dict, or None)."""
    if tags_raw is None:
        return None
    if isinstance(tags_raw, float):  # NaN
        return None
    if isinstance(tags_raw, dict):
        tags = tags_raw
    else:
        s = str(tags_raw).strip()
        if not s or s in ("nan", "None", "{}"):
            return None
        try:
            tags = json.loads(s)
        except (json.JSONDecodeError, ValueError):
            return None
    if not isinstance(tags, dict):
        return None
    key_lower = _TAG_KEY.lower()
    for k, v in tags.items():
        if k.lower() == key_lower and v:
            raw = str(v).strip().upper()
            if "," in raw:
                for token in (t.strip() for t in raw.split(",")):
                    if token and token != _COMPARTILHADO:
                        return token
                return _COMPARTILHADO  # todos os tokens eram COMPARTILHADO ou vazios
            return raw
    return None


def _mc_cluster_vec(rg_series: pd.Series, cluster_names: list[str]) -> pd.Series:
    """Vectorized MC_* RG → cluster name. Returns None for non-matching rows."""
    result = pd.Series([None] * len(rg_series), index=rg_series.index, dtype=object)
    mc_mask = rg_series.str.upper().str.startswith("MC_", na=False)
    if not mc_mask.any():
        return result
    rg_lower = rg_series.str.lower()
    for cluster in sorted(cluster_names, key=len, reverse=True):
        found = mc_mask & rg_lower.str.contains(cluster.lower(), regex=False, na=False)
        result[found & result.isna()] = cluster
    return result


def run(pipeline_date: Optional[str] = None) -> None:
    date_str = resolve_pipeline_date(pipeline_date)
    logger.info("📅 s02_classify  date=%s", date_str)

    store = pipeline_client()

    df = store.read_parquet(
        f"{_FACT_COST_PREFIX}/{to_partition(date_str)}/fact_cost.parquet"
    )
    n_input = len(df)
    logger.info("📊 fact_cost: %d linhas", n_input)

    # rg_tags → {RG_UPPER: FINOPS_CLIENTE_UPPER}
    rg_tags_map: dict[str, str] = {}
    try:
        rg_df = store.read_csv(f"{_REFERENCE_PREFIX}/rg_tags.csv")
        if not rg_df.empty:
            for _, row in rg_df.iterrows():
                rg_tags_map[str(row["ResourceGroupName"]).strip().upper()] = (
                    str(row["FinopsCliente"]).strip().upper()
                )
        logger.info("📋 rg_tags: %d entradas", len(rg_tags_map))
    except Exception as exc:
        logger.warning("⚠ rg_tags.csv indisponível: %s", exc)

    # cluster_config → shared cluster names
    cluster_cfg = store.read_csv(f"{_REFERENCE_PREFIX}/cluster_config.csv")
    shared_clusters = cluster_cfg[cluster_cfg["Tipo"] == "shared"]["ClusterName"].tolist()

    # overrides_rg_auto → {RG_UPPER: cluster_name}
    overrides_map: dict[str, str] = {}
    try:
        ov_df = store.read_csv(f"{_REFERENCE_PREFIX}/overrides_rg_auto.csv")
        if not ov_df.empty and {"ResourceGroupName", "ClusterName"}.issubset(ov_df.columns):
            for _, row in ov_df.iterrows():
                if pd.notna(row.get("ClusterName")):
                    overrides_map[str(row["ResourceGroupName"]).strip().upper()] = str(row["ClusterName"])
    except Exception as exc:
        logger.warning("⚠ overrides_rg_auto.csv indisponível: %s", exc)

    # ── Vetores base ───────────────────────────────────────────────────────
    rg_upper = df["ResourceGroupName"].fillna("").str.strip().str.upper()
    rid = df["ResourceId"].fillna("").astype(str)

    # Tag por recurso (JSON parsing — apply é mais rápido que iterrows)
    tag_val = df["Tags"].apply(_extract_finops_tag)

    # Tag do RG (lookup vetorizado por dicionário)
    rg_tag_cliente = rg_upper.map(rg_tags_map)

    # AKS cluster: override explícito > padrão MC_*
    rg_override = rg_upper.map(overrides_map)
    rg_mc = _mc_cluster_vec(df["ResourceGroupName"].fillna(""), shared_clusters)
    aks_cluster = rg_override.combine_first(rg_mc)

    # ── Condições de prioridade (np.select usa a primeira verdadeira) ──────
    cond1 = tag_val.notna() & (tag_val != _COMPARTILHADO)
    cond2 = rg_tag_cliente.notna() & (rg_tag_cliente != _COMPARTILHADO)
    cond3 = rg_tag_cliente.notna() & (rg_tag_cliente == _COMPARTILHADO)
    cond4 = aks_cluster.notna()
    cond5 = rg_upper != ""
    condlist = [cond1, cond2, cond3, cond4, cond5]

    # ── Colunas simples via np.select ──────────────────────────────────────
    rule = np.select(
        condlist,
        ["TAG_RESOURCE", "TAG_RG_INHERITED", "TAG_RG_SHARED", "OVERRIDE_RG_AUTO", "RG_INFERRED"],
        default="UNKNOWN",
    )
    service_type = np.select(
        condlist,
        ["dedicated", "dedicated", "shared", "aks", "shared"],
        default="unallocated",
    )
    confidence = np.select(condlist, [100, 95, 90, 95, 70], default=0)
    source = np.select(
        condlist,
        ["resource_tag", "rg_tag", "rg_tag", "mc_rg_pattern", "rg_name"],
        default="",
    )

    # ── effective_cliente: fonte varia por regra ───────────────────────────
    effective_cliente = pd.Series(
        np.select(
            condlist,
            [
                tag_val.fillna("").values,
                rg_tag_cliente.fillna("").values,
                np.full(n_input, _COMPARTILHADO),
                np.full(n_input, ""),
                np.full(n_input, _COMPARTILHADO),
            ],
            default="",
        ),
        index=df.index,
        dtype=str,
    )

    # ── service_key e notes: apenas Rule 4 altera o padrão (ResourceId) ───
    # OVERRIDE_RG_AUTO tem prioridade sobre TAG_RG_SHARED (cond3):
    # RGs MC_* são node pools AKS — ignorar tag COMPARTILHADO do rg_tags.csv
    rule4_wins = cond4 & ~cond1 & ~cond2
    service_key = rid.where(~rule4_wins, "AKS:" + aks_cluster.fillna(""))
    notes = pd.Series("", index=df.index, dtype=str).where(
        ~rule4_wins, "cluster=" + aks_cluster.fillna("")
    )

    # ── SQL Elastic Pool override: SQLPOOL:{pool_name} ────────────────────
    # FOCUS usa display name "SQL elastic pool"; ARM usa "microsoft.sql/servers/elasticpools"
    is_elasticpool = df["ResourceType"].str.lower().isin([
        "microsoft.sql/servers/elasticpools",
        "sql elastic pool",
    ])
    if is_elasticpool.any():
        pool_name_extracted = rid.str.extract(r"/elasticPools/([^/]+)$", flags=re.IGNORECASE)[0]
        valid_pool = is_elasticpool & pool_name_extracted.notna()
        service_key = service_key.where(
            ~valid_pool,
            "SQLPOOL:" + pool_name_extracted.fillna("").str.lower(),
        )
        service_type_s = pd.Series(service_type, index=df.index)
        service_type_s[valid_pool] = "sql_pool"
        service_type = service_type_s.values

    # ── Recovery Services Vault override: VAULT:{ResourceName} ───────────
    # Vault centralizado — ServiceKey por nome permite rateio em s03d por protected items
    is_vault = df["ResourceType"].str.lower().isin([
        "microsoft.recoveryservices/vaults",
        "recovery services vault",
    ])
    if is_vault.any():
        vault_name_col = df["ResourceName"].fillna("")
        valid_vault = is_vault & (vault_name_col != "")
        service_key = service_key.where(~valid_vault, "VAULT:" + vault_name_col)
        service_type_s = pd.Series(service_type, index=df.index)
        service_type_s[valid_vault] = "backup_vault"
        service_type = service_type_s.values

    # ── Snapshot service type marker ──────────────────────────────────────
    is_snapshot = df["ResourceType"].str.lower().isin([
        "microsoft.compute/snapshots",
        "snapshot",
    ])
    if is_snapshot.any():
        service_type_s = pd.Series(service_type, index=df.index)
        service_type_s[is_snapshot] = "snapshot"
        service_type = service_type_s.values

    # ── Monta output ───────────────────────────────────────────────────────
    df_out = pd.DataFrame({
        "Date": date_str,
        "ResourceId": rid,
        "ResourceType": df["ResourceType"].fillna("").astype(str),
        "ResourceName": df["ResourceName"].fillna("").astype(str),
        "ResourceGroupName": df["ResourceGroupName"].fillna("").astype(str),
        "ServiceKey": service_key,
        "ServiceType": service_type,
        "Rule": rule,
        "Confidence": confidence,
        "EffectiveFinopsCliente": effective_cliente,
        "FinopsClienteSource": source,
        "Notes": notes,
    })[_OUTPUT_COLUMNS]

    assert len(df_out) == n_input, f"Row count mismatch: {len(df_out)} != {n_input}"

    logger.info("📋 Regras: %s", df_out["Rule"].value_counts().to_dict())
    logger.info("📋 ServiceTypes: %s", df_out["ServiceType"].value_counts().to_dict())

    blob_name = f"{_R2S_PREFIX}/{to_partition(date_str)}/resource_to_service.parquet"
    store.write_parquet(blob_name, df_out)
    logger.info("✅ resource_to_service: %d linhas → %s", len(df_out), blob_name)
