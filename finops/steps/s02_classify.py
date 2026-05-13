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
from typing import Optional

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
            return str(v).strip().upper()
    return None


def _mc_cluster(rg_name: str, cluster_names: list[str]) -> Optional[str]:
    """Match MC_*_{cluster}_{region} RG name to a known cluster name."""
    if not rg_name.upper().startswith("MC_"):
        return None
    rg_lower = rg_name.lower()
    # Sort by length descending to prefer more specific matches
    for cluster in sorted(cluster_names, key=len, reverse=True):
        if cluster.lower() in rg_lower:
            return cluster
    return None


def run(pipeline_date: Optional[str] = None) -> None:
    date_str = resolve_pipeline_date(pipeline_date)
    logger.info("📅 s02_classify  date=%s", date_str)

    store = pipeline_client()

    df = store.read_parquet(
        f"{_FACT_COST_PREFIX}/{to_partition(date_str)}/fact_cost.parquet"
    )
    logger.info("📊 fact_cost: %d linhas", len(df))

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
    except Exception:
        pass

    rows: list[dict] = []

    for _, r in df.iterrows():
        rid = str(r.get("ResourceId", "") or "")
        rg = str(r.get("ResourceGroupName", "") or "")
        rg_upper = rg.strip().upper()

        service_key = rid
        service_type = "unallocated"
        rule = "UNKNOWN"
        confidence = 0
        effective_cliente = ""
        source = ""
        notes = ""

        # Rule 1 — tag FINOPS-CLIENTE no recurso
        tag_val = _extract_finops_tag(r.get("Tags"))
        if tag_val and tag_val != _COMPARTILHADO:
            effective_cliente = tag_val
            service_type = "dedicated"
            rule = "TAG_RESOURCE"
            confidence = 100
            source = "resource_tag"

        # Rule 2 — tag no RG, cliente dedicado
        elif rg_upper in rg_tags_map and rg_tags_map[rg_upper] != _COMPARTILHADO:
            effective_cliente = rg_tags_map[rg_upper]
            service_type = "dedicated"
            rule = "TAG_RG_INHERITED"
            confidence = 95
            source = "rg_tag"

        # Rule 3 — tag no RG = COMPARTILHADO
        elif rg_upper in rg_tags_map and rg_tags_map[rg_upper] == _COMPARTILHADO:
            effective_cliente = _COMPARTILHADO
            service_type = "shared"
            rule = "TAG_RG_SHARED"
            confidence = 90
            source = "rg_tag"

        else:
            # Rule 4 — RG padrão MC_* (node pool de cluster AKS)
            cluster = overrides_map.get(rg_upper) or _mc_cluster(rg, shared_clusters)
            if cluster:
                service_key = f"AKS:{cluster}"
                service_type = "aks"
                rule = "OVERRIDE_RG_AUTO"
                confidence = 95
                effective_cliente = ""
                source = "mc_rg_pattern"
                notes = f"cluster={cluster}"

            # Rule 5 — RG existe mas sem tag → compartilhado por inferência
            elif rg_upper:
                effective_cliente = _COMPARTILHADO
                service_type = "shared"
                rule = "RG_INFERRED"
                confidence = 70
                source = "rg_name"

            # Rule 6 — sem classificação
            else:
                rule = "UNKNOWN"
                service_type = "unallocated"

        rows.append({
            "Date": date_str,
            "ResourceId": rid,
            "ResourceType": str(r.get("ResourceType", "") or ""),
            "ResourceName": str(r.get("ResourceName", "") or ""),
            "ResourceGroupName": rg,
            "ServiceKey": service_key,
            "ServiceType": service_type,
            "Rule": rule,
            "Confidence": confidence,
            "EffectiveFinopsCliente": effective_cliente,
            "FinopsClienteSource": source,
            "Notes": notes,
        })

    df_out = pd.DataFrame(rows, columns=_OUTPUT_COLUMNS)

    logger.info("📋 Regras: %s", df_out["Rule"].value_counts().to_dict())
    logger.info("📋 ServiceTypes: %s", df_out["ServiceType"].value_counts().to_dict())

    blob_name = f"{_R2S_PREFIX}/{to_partition(date_str)}/resource_to_service.parquet"
    store.write_parquet(blob_name, df_out)
    logger.info("✅ resource_to_service: %d linhas → %s", len(df_out), blob_name)
