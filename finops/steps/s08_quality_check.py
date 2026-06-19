"""
s08_quality_check.py — Verificação de qualidade pós-alocação.

Agrega dois tipos de pendências de qualidade e registra no log + blob:

  1. NAMESPACE_SEM_ALIAS — namespaces AKS não mapeados para cliente.
     Fonte: namespaces_sem_alias_{date}.csv gerados por s03b em cada cluster.
     Ação: adicionar entradas em config/reference/clientes_alias.csv.

  2. RECURSO_NAO_ALOCADO — recursos sem ServiceKey ou sem share.
     Fonte: unallocated_cost_{date}.csv gerado por s06.
     Reasons monitorados: NO_SERVICEKEY, HAS_SERVICEKEY_NO_SHARE.
     Ação: adicionar tag FINOPS-CLIENTE no recurso ou mapear em s02.

Saídas (gold/quality_alerts/dt=<date>/):
  namespaces_sem_alias_{date}.csv    — todos os clusters consolidados
  recursos_nao_alocados_{date}.csv   — subset actionable de unallocated_cost
"""

import logging
import os
from typing import Optional

import pandas as pd

from finops.storage import pipeline_client, upload_csv
from finops.utils.dates import resolve_pipeline_date, to_partition

logger = logging.getLogger(__name__)

_AKS_OUT_PREFIX = os.getenv("AKS_OUT_PREFIX", "gold/aks_allocation_share_opencost")
_FACT_PREFIX = os.getenv("FACT_ALLOCATED_COST_PREFIX", "gold/fact_allocated_cost")
_QUALITY_PREFIX = os.getenv("QUALITY_ALERTS_PREFIX", "gold/quality_alerts")

_ACTIONABLE_REASONS = {"NO_SERVICEKEY", "HAS_SERVICEKEY_NO_SHARE"}


def run(pipeline_date: Optional[str] = None) -> None:
    date_str = resolve_pipeline_date(pipeline_date)
    logger.info("📅 s08_quality_check  date=%s", date_str)

    store = pipeline_client()
    storage_account = os.environ["PIPELINE_STORAGE_ACCOUNT"]
    container = os.environ["PIPELINE_CONTAINER"]
    partition = to_partition(date_str)
    has_alerts = False

    # ── Part 1: Namespaces AKS sem alias ─────────────────────────────────────
    aks_blobs = store.list_blobs(f"{_AKS_OUT_PREFIX}/{partition}/")
    alias_blobs = [b for b in aks_blobs if "namespaces_sem_alias_" in b.split("/")[-1]]

    df_ns = pd.DataFrame()
    if alias_blobs:
        frames = [store.read_csv(b) for b in alias_blobs]
        df_ns = pd.concat(frames, ignore_index=True)

    if not df_ns.empty:
        n_ns = len(df_ns)
        cost_ns = float(df_ns["UnmappedCost"].sum()) if "UnmappedCost" in df_ns.columns else 0.0
        n_clusters = df_ns["Cluster"].nunique() if "Cluster" in df_ns.columns else len(alias_blobs)

        if "Cluster" in df_ns.columns and "Namespace" in df_ns.columns:
            sorted_ns = df_ns.sort_values(["Cluster", "Namespace"])
            ns_compact = "; ".join(
                "{}/{}(R${:.2f}){}".format(
                    row["Cluster"],
                    row["Namespace"],
                    row.get("UnmappedCost", 0),
                    "[?sistema]" if row.get("CandidatoSistema") else "",
                )
                for _, row in sorted_ns.iterrows()
            )
        else:
            ns_compact = "(detalhes indisponíveis)"

        logger.warning(
            "⚠ NAMESPACE_SEM_ALIAS: %d namespaces em %d cluster(s) | custo redistribuído=R$%.2f"
            " | Ocorrências: %s | Ação: adicionar em config/reference/clientes_alias.csv",
            n_ns, n_clusters, cost_ns, ns_compact,
        )
        blob_ns = f"{_QUALITY_PREFIX}/{partition}/namespaces_sem_alias_{date_str}.csv"
        upload_csv(df_ns, storage_account, container, blob_ns)
        logger.info("📋 Namespaces sem alias consolidados → %s", blob_ns)
        has_alerts = True
    else:
        logger.info("✅ Nenhum namespace AKS sem alias em %s", date_str)

    # ── Part 2: Recursos não alocados (actionable) ────────────────────────────
    unalloc_blob = f"{_FACT_PREFIX}/{partition}/unallocated_cost_{date_str}.csv"
    df_unalloc = pd.DataFrame()
    try:
        df_raw = store.read_csv(unalloc_blob)
        df_unalloc = df_raw[df_raw["Reason"].isin(_ACTIONABLE_REASONS)].copy()
    except Exception as exc:
        logger.warning("⚠ unallocated_cost não encontrado para %s: %s", date_str, exc)

    if not df_unalloc.empty:
        n_res = len(df_unalloc)
        cost_res = float(df_unalloc["Cost"].sum()) if "Cost" in df_unalloc.columns else 0.0
        by_reason = df_unalloc.groupby("Reason").size().to_dict()
        logger.warning(
            "⚠ RECURSO_NAO_ALOCADO: %d recursos | custo=R$%.2f | por motivo: %s"
            " | Ação: tag FINOPS-CLIENTE no recurso ou mapear ServiceKey em s02",
            n_res, cost_res, by_reason,
        )
        blob_res = f"{_QUALITY_PREFIX}/{partition}/recursos_nao_alocados_{date_str}.csv"
        upload_csv(df_unalloc, storage_account, container, blob_res)
        logger.info("📋 Recursos não alocados → %s", blob_res)
        has_alerts = True
    else:
        logger.info("✅ Nenhum recurso não alocado (actionable) em %s", date_str)

    # ── Part 3: Clusters AKS com shares cacheados (OpenCost estava indisponível) ──
    aks_partition_blobs = store.list_blobs(f"{_AKS_OUT_PREFIX}/{partition}/")
    cluster_blobs = [
        b for b in aks_partition_blobs
        if not b.split("/")[-1].startswith("namespaces_sem_alias")
    ]
    cached_clusters: list[str] = []
    for blob in cluster_blobs:
        try:
            df_aks = store.read_csv(blob)
            if "AllocationMethod" in df_aks.columns and (
                df_aks["AllocationMethod"] == "AKS_OPENCOST_CACHED"
            ).any():
                cluster_id = blob.split("/")[-1].replace(".csv", "")
                cached_clusters.append(cluster_id)
        except Exception:
            pass

    if cached_clusters:
        cached_compact = "; ".join(cached_clusters)
        logger.warning(
            "⚠ AKS_OPENCOST_CACHED: %d cluster(s) usando shares históricos (OpenCost indisponível)"
            " | Clusters: %s | Ação: verificar pods OpenCost nesses clusters",
            len(cached_clusters), cached_compact,
        )
        has_alerts = True
    else:
        logger.info("✅ Todos os clusters AKS com shares em tempo real em %s", date_str)

    if has_alerts:
        logger.warning(
            "⚠ s08 concluído com alertas para %s — ver gold/quality_alerts/%s/",
            date_str, partition,
        )
    else:
        logger.info("✅ s08_quality_check concluído — sem alertas para %s", date_str)
