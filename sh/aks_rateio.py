#!/usr/bin/env python3
"""
aks_rateio.py — Lógica de rateio AKS/OpenCost (standalone).

Chama a API OpenCost, separa namespaces de sistema dos de cliente,
redistribui o custo de infra proporcionalmente e grava os shares em CSV.

Uso:
    python sh/aks_rateio.py --date 2026-11-10 --cluster aks-prd-newkmm \
        --endpoint http://10.152.26.15:9003 --alias config/reference/clientes_alias.csv \
        --system config/reference/aks_system_namespaces.csv --out shares.csv

Variáveis de ambiente (alternativa aos flags):
    OPENCOST_ENDPOINT, CLUSTER_NAME, PIPELINE_DATE,
    OPENCOST_TIMEOUT_SEC, OPENCOST_RETRY_COUNT, OPENCOST_RETRY_DELAY_SEC
"""

import argparse
import csv
import os
import sys
import time
from datetime import date, timedelta
from io import StringIO
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT = int(os.getenv("OPENCOST_TIMEOUT_SEC", "60"))
DEFAULT_RETRIES = int(os.getenv("OPENCOST_RETRY_COUNT", "2"))
DEFAULT_DELAY   = float(os.getenv("OPENCOST_RETRY_DELAY_SEC", "5"))

SYSTEM_PATTERNS = {
    "kube", "monitoring", "logging", "cert-manager", "ingress", "nginx",
    "flux", "argocd", "istio", "linkerd", "prometheus", "grafana",
    "elastic", "loki", "jaeger", "vault", "external-secrets", "sealed-secrets",
    "crossplane", "velero", "trivy", "falco", "gatekeeper", "opa", "tekton",
    "calico", "cilium", "coredns", "metrics-server", "cluster-autoscaler",
    "reloader", "reflector", "kubeops", "opencost", "finops",
}

# ---------------------------------------------------------------------------
# OpenCost
# ---------------------------------------------------------------------------

def fetch_namespace_costs(
    endpoint: str,
    date_str: str,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    delay: float = DEFAULT_DELAY,
) -> dict[str, float]:
    """Retorna {namespace: totalCost} para o dia informado via OpenCost API."""
    url = f"{endpoint.rstrip('/')}/allocation/compute"
    params = {
        "window":    f"{date_str}T00:00:00Z,{date_str}T23:59:59Z",
        "aggregate": "namespace",
        "accumulate": "true",
        "includeIdle": "true",
    }
    last_exc: Exception | None = None
    for attempt in range(1, retries + 2):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json().get("data") or []
            merged: dict = {}
            for entry in data:
                merged.update(entry)
            return {ns: alloc.get("totalCost", 0.0) for ns, alloc in merged.items()}
        except Exception as exc:
            last_exc = exc
            if attempt <= retries:
                wait = delay * (2 ** (attempt - 1))
                print(f"⚠  Tentativa {attempt} falhou: {exc} — aguardando {wait:.1f}s", file=sys.stderr)
                time.sleep(wait)
    raise RuntimeError(f"OpenCost indisponível após {retries + 1} tentativas: {last_exc}")


# ---------------------------------------------------------------------------
# Classificação de namespace
# ---------------------------------------------------------------------------

def load_csv(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh, delimiter=";"))


def is_system(ns: str, system_rows: list[dict]) -> bool:
    """Idle buckets (__*) e namespaces listados em aks_system_namespaces.csv."""
    if ns.startswith("__"):
        return True
    ns_lower = ns.lower()
    for row in system_rows:
        pattern = row["Namespace"].lower()
        if row["MatchMode"] == "exact" and ns_lower == pattern:
            return True
        if row["MatchMode"] == "contains" and pattern in ns_lower:
            return True
    return False


def is_system_heuristic(ns: str) -> bool:
    """Heurística por nome: namespace de infra/plataforma não mapeado."""
    ns_lower = ns.lower()
    return any(p in ns_lower for p in SYSTEM_PATTERNS)


def resolve_client(ns: str, alias_rows: list[dict]) -> Optional[str]:
    """Retorna nome do cliente pelo alias exato ou por substring."""
    ns_lower = ns.lower()
    for row in alias_rows:
        if row["Alias"].lower() == ns_lower:
            return row["Cliente"]
    for row in alias_rows:
        if row["Alias"].lower() in ns_lower:
            return row["Cliente"]
    return None


# ---------------------------------------------------------------------------
# Rateio principal
# ---------------------------------------------------------------------------

def calcular_shares(
    ns_costs: dict[str, float],
    alias_rows: list[dict],
    system_rows: list[dict],
    cluster_name: str,
    date_str: str,
) -> tuple[list[dict], list[dict]]:
    """
    Retorna (shares, unmapped):
      shares   — lista de {Date, ServiceKey, Cliente, Share, AllocationMethod, DriverType}
      unmapped — namespaces de negócio sem alias (para auditoria)
    """
    ns_costs = {ns: c for ns, c in ns_costs.items() if c > 0}
    if not ns_costs:
        raise ValueError("Sem dados OpenCost com custo > 0")

    service_key = f"AKS:{cluster_name}"

    # 1. Separar sistema vs negócio
    system_cost = 0.0
    business: dict[str, float] = {}
    for ns, cost in ns_costs.items():
        if is_system(ns, system_rows):
            system_cost += cost
        else:
            business[ns] = cost

    # 2. Mapear namespace → cliente; não-mapeados vão para balde de sistema
    client_costs: dict[str, float] = {}
    unmapped_cost = 0.0
    unmapped: list[dict] = []

    for ns, cost in business.items():
        cliente = resolve_client(ns, alias_rows)
        if cliente and cliente.upper() not in ("COMPARTILHADO", "DEFAULT"):
            client_costs[cliente] = client_costs.get(cliente, 0.0) + cost
        else:
            unmapped_cost += cost
            unmapped.append({
                "Cluster":          cluster_name,
                "Namespace":        ns,
                "CustoUSD":         round(cost, 6),
                "AliasResolvido":   cliente or "",
                "CandidatoSistema": is_system_heuristic(ns),
            })

    mapped_total = sum(client_costs.values())
    if mapped_total == 0:
        raise ValueError("Nenhum namespace mapeado para cliente — verifique clientes_alias.csv")

    # 3. Redistribuir custo de sistema proporcionalmente aos clientes mapeados
    total_system = system_cost + unmapped_cost
    fator = 1.0 + (total_system / mapped_total)
    client_costs_adj = {c: v * fator for c, v in client_costs.items()}
    grand_total = sum(client_costs_adj.values())

    shares = [
        {
            "Date":             date_str,
            "ServiceKey":       service_key,
            "Cliente":          cliente,
            "Share":            round(cost / grand_total, 6),
            "AllocationMethod": "AKS_OPENCOST",
            "DriverType":       "OPENCOST_NAMESPACE",
        }
        for cliente, cost in sorted(client_costs_adj.items(), key=lambda x: -x[1])
    ]

    # Validação
    soma = sum(r["Share"] for r in shares)
    if abs(soma - 1.0) > 0.001:
        print(f"⚠  Soma dos shares: {soma:.6f} (esperado 1.0)", file=sys.stderr)

    return shares, unmapped


# ---------------------------------------------------------------------------
# Saída CSV
# ---------------------------------------------------------------------------

def write_csv(rows: list[dict], path: str) -> None:
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()), delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def print_shares(shares: list[dict]) -> None:
    header = f"{'Cliente':<30} {'Share':>10} {'%':>8}"
    print(header)
    print("-" * len(header))
    for r in shares:
        pct = r["Share"] * 100
        print(f"{r['Cliente']:<30} {r['Share']:>10.6f} {pct:>7.2f}%")
    soma = sum(r["Share"] for r in shares)
    print("-" * len(header))
    print(f"{'TOTAL':<30} {soma:>10.6f} {soma*100:>7.2f}%")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _resolve_date(raw: Optional[str]) -> str:
    if raw:
        return raw
    env = os.getenv("PIPELINE_DATE", "").strip()
    if env:
        return env
    return str(date.today() - timedelta(days=3))


def main() -> None:
    ap = argparse.ArgumentParser(description="Rateio AKS/OpenCost standalone")
    ap.add_argument("--date",     default=None, help="Data yyyy-MM-dd (default: D-3)")
    ap.add_argument("--cluster",  default=os.getenv("CLUSTER_NAME"), required=not os.getenv("CLUSTER_NAME"))
    ap.add_argument("--endpoint", default=os.getenv("OPENCOST_ENDPOINT"), required=not os.getenv("OPENCOST_ENDPOINT"))
    ap.add_argument("--alias",    default="config/reference/clientes_alias.csv")
    ap.add_argument("--system",   default="config/reference/aks_system_namespaces.csv")
    ap.add_argument("--out",      default=None, help="Arquivo de saída CSV (opcional; default: imprime na tela)")
    ap.add_argument("--unmapped-out", default=None, help="CSV com namespaces sem alias (opcional)")
    ap.add_argument("--timeout",  type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("--retries",  type=int, default=DEFAULT_RETRIES)
    args = ap.parse_args()

    date_str = _resolve_date(args.date)
    print(f"📅 date={date_str}  cluster={args.cluster}  endpoint={args.endpoint}")

    # Carregar referências
    alias_rows  = load_csv(args.alias)
    system_rows = load_csv(args.system)
    print(f"📋 {len(alias_rows)} aliases · {len(system_rows)} regras de sistema")

    # Buscar OpenCost
    print(f"⬇️  Consultando OpenCost...")
    ns_costs = fetch_namespace_costs(
        args.endpoint, date_str,
        timeout=args.timeout, retries=args.retries,
    )
    print(f"🔢 {len(ns_costs)} namespaces recebidos (total com custo > 0: {sum(1 for c in ns_costs.values() if c > 0)})")

    # Calcular shares
    shares, unmapped = calcular_shares(ns_costs, alias_rows, system_rows, args.cluster, date_str)
    print(f"✅ {len(shares)} clientes mapeados · {len(unmapped)} namespaces sem alias\n")

    # Saída
    if args.out:
        write_csv(shares, args.out)
        print(f"⬆️  Shares gravados em: {args.out}")
    else:
        print_shares(shares)

    if unmapped and args.unmapped_out:
        write_csv(unmapped, args.unmapped_out)
        print(f"📋 Namespaces sem alias gravados em: {args.unmapped_out}")
    elif unmapped:
        print(f"\n⚠  {len(unmapped)} namespaces sem alias:")
        for r in unmapped:
            flag = " [infra?]" if r["CandidatoSistema"] else ""
            print(f"   {r['Namespace']:<40} USD {r['CustoUSD']:>10.4f}{flag}")


if __name__ == "__main__":
    main()
