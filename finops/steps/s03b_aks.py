"""
s03b_aks.py — Alocação AKS por namespace via OpenCost.

Roda dentro de cada cluster compartilhado (05:00 UTC).
Lê CLUSTER_NAME e OPENCOST_ENDPOINT do ambiente, chama a API de alocação
por namespace, redistribui o custo de namespaces de sistema, resolve
namespace → cliente via label K8s `finops-cliente` (primário) ou
clientes_alias.csv (fallback) e grava os shares no storage.

Resolução de namespace → cliente:
  1. Label `finops-cliente` do namespace (K8s API)
     - Clusters locais: kubernetes Python client (in-cluster)
     - Clusters remotos: ContainerService run_command (env REMOTE_AKS_RG/SUBSCRIPTION)
  2. clientes_alias.csv (fallback para namespaces sem label)
  3. Bucket de sistema (redistribuído proporcionalmente aos mapeados)
"""

import json
import logging
import os
import re
import time
from typing import Optional

import requests
import pandas as pd

from finops.storage import pipeline_client
from finops.utils.dates import resolve_pipeline_date, to_partition

logger = logging.getLogger(__name__)

_LABEL_KEY = "finops-cliente"

_REFERENCE_PREFIX = os.getenv("REFERENCE_PREFIX", "config/reference")
_AKS_OUT_PREFIX = os.getenv("AKS_OUT_PREFIX", "gold/aks_allocation_share_opencost")

_OC_TIMEOUT = int(os.getenv("OPENCOST_TIMEOUT_SEC", "60"))
_OC_RETRIES = int(os.getenv("OPENCOST_RETRY_COUNT", "2"))
_OC_DELAY = float(os.getenv("OPENCOST_RETRY_DELAY_SEC", "5"))


# ------------------------------------------------------------------
# OpenCost helpers
# ------------------------------------------------------------------

def _fetch_allocations(endpoint: str, date_str: str) -> dict[str, float]:
    """Return {namespace: totalCost} for the target day via OpenCost API."""
    url = f"{endpoint.rstrip('/')}/allocation/compute"
    params = {
        "window": f"{date_str}T00:00:00Z,{date_str}T23:59:59Z",
        "aggregate": "namespace",
        "accumulate": "true",
        "includeIdle": "true",
    }
    last_exc: Exception | None = None
    for attempt in range(1, _OC_RETRIES + 2):
        try:
            resp = requests.get(url, params=params, timeout=_OC_TIMEOUT)
            resp.raise_for_status()
            data = resp.json().get("data") or []
            merged: dict = {}
            for entry in data:
                merged.update(entry)
            return {ns: alloc.get("totalCost", 0.0) for ns, alloc in merged.items()}
        except Exception as exc:
            last_exc = exc
            if attempt <= _OC_RETRIES:
                wait = _OC_DELAY * (2 ** (attempt - 1))
                logger.warning("⚠ OpenCost tentativa %d falhou: %s — aguardando %.1fs", attempt, exc, wait)
                time.sleep(wait)
    raise last_exc  # type: ignore[misc]


# ------------------------------------------------------------------
# Cached share fallback
# ------------------------------------------------------------------

def _load_last_known_shares(
    store, prefix: str, cluster_name: str, current_date: str
) -> tuple[Optional[pd.DataFrame], Optional[str]]:
    """Return (df, last_date) using the most recent valid share for this cluster.

    Looks back up to 7 days. Returns (None, None) if no history exists.
    Used when OpenCost is unavailable so allocation stays fair (only charges
    clients who actually run workloads on this cluster).
    """
    try:
        all_blobs = store.list_blobs(f"{prefix}/")
        cluster_file = f"/{cluster_name}.csv"
        candidates = sorted(
            [
                b for b in all_blobs
                if b.endswith(cluster_file) and f"dt={current_date}" not in b
            ],
            reverse=True,
        )
        if not candidates:
            return None, None
        latest_blob = candidates[0]
        m = re.search(r"dt=(\d{4}-\d{2}-\d{2})", latest_blob)
        last_date = m.group(1) if m else "desconhecido"
        df = store.read_csv(latest_blob)
        return df, last_date
    except Exception as exc:
        logger.warning("⚠ Não foi possível carregar shares históricos para %s: %s", cluster_name, exc)
        return None, None


# ------------------------------------------------------------------
# K8s namespace label resolution
# ------------------------------------------------------------------

def _fetch_labels_incluster() -> dict[str, str]:
    """Busca labels via kubernetes Python client (in-cluster config)."""
    try:
        from kubernetes import client as k8s_client, config as k8s_config
        k8s_config.load_incluster_config()
        v1 = k8s_client.CoreV1Api()
        ns_list = v1.list_namespace()
        result = {}
        for ns in ns_list.items:
            labels = ns.metadata.labels or {}
            cliente = labels.get(_LABEL_KEY)
            if cliente:
                result[ns.metadata.name] = cliente
        logger.info("🏷️  Labels in-cluster: %d namespaces com %s", len(result), _LABEL_KEY)
        return result
    except Exception as exc:
        logger.warning("⚠ Falha ao ler labels K8s in-cluster: %s — usando somente CSV", exc)
        return {}


def _fetch_labels_remote(cluster_name: str, resource_group: str, subscription_id: str) -> dict[str, str]:
    """Busca labels via ContainerService run_command (clusters remotos sem Workload Identity local)."""
    try:
        from azure.mgmt.containerservice import ContainerServiceClient
        from azure.mgmt.containerservice.models import RunCommandRequest
        from azure.identity import DefaultAzureCredential

        cred = DefaultAzureCredential()
        cs_client = ContainerServiceClient(cred, subscription_id)
        poller = cs_client.managed_clusters.begin_run_command(
            resource_group_name=resource_group,
            resource_name=cluster_name,
            request_parameters=RunCommandRequest(command="kubectl get ns -o json"),
        )
        result = poller.result(timeout=120)
        if result.exit_code != 0:
            raise RuntimeError(f"kubectl exit {result.exit_code}: {(result.logs or '')[:300]}")

        ns_data = json.loads(result.logs or "{}")
        label_map = {}
        for ns in ns_data.get("items", []):
            labels = (ns.get("metadata") or {}).get("labels") or {}
            cliente = labels.get(_LABEL_KEY)
            if cliente:
                label_map[ns["metadata"]["name"]] = cliente

        logger.info("🏷️  Labels remote run_command: %d namespaces com %s", len(label_map), _LABEL_KEY)
        return label_map
    except Exception as exc:
        logger.warning("⚠ Falha ao buscar labels via run_command em %s: %s — usando somente CSV", cluster_name, exc)
        return {}


def _fetch_namespace_labels(cluster_name: str) -> dict[str, str]:
    """
    Retorna {namespace: finops-cliente} detectando o modo automaticamente:
      REMOTE_AKS_RG definido  → ContainerService run_command (cluster remoto)
      KUBERNETES_SERVICE_HOST → kubernetes Python client (in-cluster)
      Nenhum dos dois         → dict vazio (somente CSV)
    """
    remote_rg  = os.getenv("REMOTE_AKS_RG", "").strip()
    remote_sub = os.getenv("REMOTE_AKS_SUBSCRIPTION", "").strip()

    if remote_rg and remote_sub:
        return _fetch_labels_remote(cluster_name, remote_rg, remote_sub)
    if os.getenv("KUBERNETES_SERVICE_HOST"):
        return _fetch_labels_incluster()
    logger.warning("⚠ Sem acesso à API K8s — resolução de namespace via CSV apenas")
    return {}


# ------------------------------------------------------------------
# Namespace classification helpers
# ------------------------------------------------------------------

def _is_system(ns: str, system_df: pd.DataFrame) -> bool:
    """Idle buckets and namespaces listed in aks_system_namespaces.csv."""
    if ns.startswith("__"):
        return True
    ns_lower = ns.lower()
    for _, row in system_df.iterrows():
        pattern = str(row["Namespace"]).lower()
        if row["MatchMode"] == "exact" and ns_lower == pattern:
            return True
        if row["MatchMode"] == "contains" and pattern in ns_lower:
            return True
    return False


_SYSTEM_CANDIDATE_PATTERNS = {
    "kube", "monitoring", "logging", "cert-manager", "ingress", "nginx",
    "flux", "argocd", "istio", "linkerd", "prometheus", "grafana",
    "elastic", "loki", "jaeger", "vault", "external-secrets", "sealed-secrets",
    "crossplane", "velero", "trivy", "falco", "gatekeeper", "opa", "tekton",
    "calico", "cilium", "coredns", "metrics-server", "cluster-autoscaler",
    "reloader", "reflector", "kubeops",
}


def _is_system_candidate(ns: str) -> bool:
    """Heuristic: namespace name suggests infra/system, not a business client."""
    ns_lower = ns.lower()
    return any(p in ns_lower for p in _SYSTEM_CANDIDATE_PATTERNS)


def _resolve_client_csv(ns: str, alias_df: pd.DataFrame) -> Optional[str]:
    """Fallback: resolve namespace → cliente via clientes_alias.csv."""
    ns_lower = ns.lower()
    exact = alias_df[alias_df["Alias"].str.lower() == ns_lower]
    if not exact.empty:
        return str(exact.iloc[0]["Cliente"])
    for _, row in alias_df.iterrows():
        if str(row["Alias"]).lower() in ns_lower:
            return str(row["Cliente"])
    return None


# ------------------------------------------------------------------
# Step entry point
# ------------------------------------------------------------------

def run(pipeline_date: Optional[str] = None) -> None:
    date_str = resolve_pipeline_date(pipeline_date)
    endpoint = os.environ["OPENCOST_ENDPOINT"].rstrip("/")
    cluster_name = os.environ["CLUSTER_NAME"]
    service_key = f"AKS:{cluster_name}"

    logger.info("📅 s03b_aks  date=%s  cluster=%s", date_str, cluster_name)

    store = pipeline_client()
    alias_df = store.read_csv(f"{_REFERENCE_PREFIX}/clientes_alias.csv")
    system_df = store.read_csv(f"{_REFERENCE_PREFIX}/aks_system_namespaces.csv")

    # Fetch namespace labels (resolução primária: label > CSV)
    ns_labels = _fetch_namespace_labels(cluster_name)

    try:
        ns_costs = _fetch_allocations(endpoint, date_str)
    except Exception as exc:
        logger.error(
            "❌ OpenCost indisponível para %s após %d tentativas: %s",
            cluster_name, _OC_RETRIES + 1, exc,
        )
        df_cached, last_date = _load_last_known_shares(store, _AKS_OUT_PREFIX, cluster_name, date_str)
        if df_cached is not None and not df_cached.empty:
            df_cached = df_cached.copy()
            df_cached["Date"] = date_str
            df_cached["AllocationMethod"] = "AKS_OPENCOST_CACHED"
            df_cached["DriverType"] = "OPENCOST_NAMESPACE_CACHED"
            blob_name = f"{_AKS_OUT_PREFIX}/{to_partition(date_str)}/{cluster_name}.csv"
            store.write_csv(blob_name, df_cached)
            logger.warning(
                "⚠ AKS_OPENCOST_CACHED: %s usando shares de %s (OpenCost indisponível)"
                " — rateio baseado em uso histórico, não em tempo real",
                cluster_name, last_date,
            )
        else:
            logger.error(
                "❌ Sem shares históricos para %s — cluster sem alocação AKS para %s",
                cluster_name, date_str,
            )
        return

    ns_costs = {ns: cost for ns, cost in ns_costs.items() if cost > 0}

    if not ns_costs:
        logger.warning("⚠ Sem dados OpenCost para %s em %s — abortando", cluster_name, date_str)
        return

    logger.info("🔢 %d namespaces recebidos (custo > 0)", len(ns_costs))

    # Separate system vs business namespaces
    system_cost = 0.0
    business: dict[str, float] = {}

    for ns, cost in ns_costs.items():
        if _is_system(ns, system_df):
            system_cost += cost
        else:
            business[ns] = cost

    business_total = sum(business.values())

    if business_total == 0:
        logger.warning("⚠ Custo de negócio = 0 para %s — abortando", cluster_name)
        return

    # Map namespace → client: label (primário) → CSV (fallback)
    client_costs: dict[str, float] = {}
    unmapped_cost = 0.0
    unmapped_ns: list[dict] = []
    n_by_label = 0
    n_by_csv = 0

    for ns, cost in business.items():
        # Prioridade 1: label K8s
        cliente = ns_labels.get(ns)
        source = "LABEL"
        if cliente and cliente.upper() in ("COMPARTILHADO", "DEFAULT"):
            cliente = None

        # Prioridade 2: CSV alias
        if not cliente:
            cliente = _resolve_client_csv(ns, alias_df)
            source = "CSV"

        if cliente:
            client_costs[cliente] = client_costs.get(cliente, 0.0) + cost
            if source == "LABEL":
                n_by_label += 1
            else:
                n_by_csv += 1
        else:
            logger.debug("Namespace sem mapeamento: %s (custo=%.6f) → sistema", ns, cost)
            unmapped_cost += cost
            unmapped_ns.append({
                "Cluster": cluster_name,
                "Namespace": ns,
                "UnmappedCost": round(cost, 6),
                "LabelResolucao": ns_labels.get(ns, ""),
                "CsvResolucao": "",
                "RedistribuidoPara": "system_bucket",
                "CandidatoSistema": _is_system_candidate(ns),
            })

    logger.info("🏷️  Resolvidos por label: %d | por CSV: %d | sem mapeamento: %d",
                n_by_label, n_by_csv, len(unmapped_ns))

    # Accumulate all system cost (infra + unmapped)
    total_system = system_cost + unmapped_cost
    mapped_total = sum(client_costs.values())

    if mapped_total == 0:
        logger.warning("⚠ Nenhum namespace mapeado para cliente em %s — abortando", cluster_name)
        return

    # Redistribute system cost proportionally to mapped clients
    redistrib_factor = 1.0 + (total_system / mapped_total)
    client_costs_adj = {c: v * redistrib_factor for c, v in client_costs.items()}
    grand_total = sum(client_costs_adj.values())

    shares = [
        {
            "Date": date_str,
            "ServiceKey": service_key,
            "Cliente": cliente,
            "Share": round(cost / grand_total, 6),
            "DriverType": "OPENCOST_NAMESPACE",
            "AllocationMethod": "AKS_OPENCOST",
        }
        for cliente, cost in client_costs_adj.items()
    ]

    df_out = pd.DataFrame(shares)

    # Defensive: remove DEFAULT rows (should never appear; namespace filter above handles it)
    default_mask = df_out["Cliente"].astype(str).str.upper() == "DEFAULT"
    if default_mask.any():
        n_def = int(default_mask.sum())
        logger.warning("⚠ s03b: %d linhas DEFAULT inesperadas — removendo e renormalizando", n_def)
        df_out = df_out[~default_mask].copy()
        s = df_out["Share"].sum()
        if s > 0:
            df_out["Share"] = (df_out["Share"] / s).round(6)

    share_sum = df_out["Share"].sum()
    logger.info("✅ %d clientes | soma share=%.6f", len(df_out), share_sum)

    if abs(share_sum - 1.0) > 0.001:
        logger.warning("⚠ Soma dos shares desvio > 0.001: %.6f", share_sum)

    blob_name = f"{_AKS_OUT_PREFIX}/{to_partition(date_str)}/{cluster_name}.csv"
    store.write_csv(blob_name, df_out)
    logger.info("⬆️  Gravado: %s", blob_name)

    if unmapped_ns:
        df_unmapped = pd.DataFrame(unmapped_ns)
        # Nome inclui o cluster: todos os s03b gravam na mesma partição e um nome único
        # por data fazia cada cluster sobrescrever a auditoria do anterior (s08 via só o último).
        blob_unmapped = (
            f"{_AKS_OUT_PREFIX}/{to_partition(date_str)}/"
            f"namespaces_sem_alias_{cluster_name}_{date_str}.csv"
        )
        store.write_csv(blob_unmapped, df_unmapped)
        logger.info("📋 Namespaces sem alias: %d → %s", len(df_unmapped), blob_unmapped)
