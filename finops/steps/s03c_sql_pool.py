"""
s03c_sql_pool.py — Alocação de SQL Elastic Pools por utilização de databases.

Para cada recurso Microsoft.Sql/servers/elasticPools não-dedicado em r2s:
  1. Lista os databases do pool via Azure Management API
  2. Obtém CPU média e Disk média via Azure Monitor (cpu_percent, storage_percent)
  3. Score por database = CPU*0.97 + Disk*0.03
  4. Fallback quando score=0: peso igual entre todos os databases (1/N)
  5. Mapeia database → cliente via tag FINOPS-CLIENTE no database
  6. Databases sem tag (ou COMPARTILHADO) redistribuídos proporcionalmente
  7. AllocatedCost = PoolDailyCost × Share (calculado a partir do fact_cost)
  8. Grava shares (sql_pool.csv) e auditoria (sqlpool_detail_all.csv)

Roda no pipeline central (06:00 UTC, aks-prd-newkmm).
A identity precisa de Reader nas subscriptions onde os SQL Servers residem.
"""

import logging
import os
import re
import time
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import requests
from azure.identity import DefaultAzureCredential

from finops.storage import pipeline_client
from finops.utils.dates import resolve_pipeline_date, to_partition

logger = logging.getLogger(__name__)

_FACT_COST_PREFIX = os.getenv("FACT_COST_PREFIX", "silver/fact_cost")
_R2S_PREFIX = os.getenv("RESOURCE_TO_SERVICE_PREFIX", "silver/resource_to_service")
_SQL_POOL_OUT_PREFIX = os.getenv("SQL_POOL_OUT_PREFIX", "gold/sql_pool_allocation_share")

_ARM_BASE = "https://management.azure.com"
_ARM_SCOPE = "https://management.azure.com/.default"
_RETRY_COUNT = 3
_RETRY_DELAY = 2.0

_ELASTIC_POOL_TYPE = "microsoft.sql/servers/elasticpools"
_SYSTEM_DBS = {"master", "tempdb", "msdb", "model"}
_TAG_KEY = "FINOPS-CLIENTE"
_COMPARTILHADO = "COMPARTILHADO"
_CPU_WEIGHT = 0.97
_DISK_WEIGHT = 0.03

_SHARE_COLUMNS = ["Date", "ServiceKey", "Cliente", "Share", "DriverType", "AllocationMethod"]
_DETAIL_COLUMNS = [
    "Date", "ServiceKey", "PoolResourceId", "Server", "Pool",
    "DatabaseName", "Cliente", "CpuAvg", "DiskAvg", "Score",
    "Share", "PoolDailyCost", "AllocatedCost",
]


# ---------------------------------------------------------------------------
# Resource ID parsing
# ---------------------------------------------------------------------------

_POOL_RE = re.compile(
    r"/subscriptions/(?P<sub>[^/]+)"
    r"/resourceGroups/(?P<rg>[^/]+)"
    r"/providers/[Mm]icrosoft\.Sql/servers/(?P<server>[^/]+)"
    r"/elasticPools/(?P<pool>[^/]+)",
    re.IGNORECASE,
)


def _parse_pool_id(rid: str) -> dict:
    m = _POOL_RE.match(rid)
    if not m:
        return {}
    return {k: m.group(k) for k in ("sub", "rg", "server", "pool")}


# ---------------------------------------------------------------------------
# Azure REST client
# ---------------------------------------------------------------------------

class _ArmClient:
    """Thin Azure Management REST wrapper with exponential-backoff retry."""

    def __init__(self) -> None:
        self._cred = DefaultAzureCredential()

    def _headers(self) -> dict:
        token = self._cred.get_token(_ARM_SCOPE).token
        return {"Authorization": f"Bearer {token}"}

    def _get(self, url: str, params: dict | None = None) -> dict:
        last_exc: Exception | None = None
        for attempt in range(1, _RETRY_COUNT + 1):
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:
                last_exc = exc
                if attempt < _RETRY_COUNT:
                    wait = _RETRY_DELAY * (2 ** (attempt - 1))
                    logger.warning("⚠ ARM tentativa %d falhou: %s — aguardando %.1fs", attempt, exc, wait)
                    time.sleep(wait)
        raise last_exc  # type: ignore[misc]

    def list_pool_databases(self, sub: str, rg: str, server: str, pool: str) -> list[dict]:
        url = (
            f"{_ARM_BASE}/subscriptions/{sub}/resourceGroups/{rg}"
            f"/providers/Microsoft.Sql/servers/{server}/elasticPools/{pool}/databases"
        )
        return self._get(url, {"api-version": "2021-11-01"}).get("value", [])

    def _get_metric(
        self, sub: str, rg: str, server: str, db: str, metric: str, date_str: str
    ) -> float:
        next_day = (date.fromisoformat(date_str) + timedelta(days=1)).isoformat()
        url = (
            f"{_ARM_BASE}/subscriptions/{sub}/resourceGroups/{rg}"
            f"/providers/Microsoft.Sql/servers/{server}/databases/{db}"
            f"/providers/microsoft.insights/metrics"
        )
        params = {
            "api-version": "2018-01-01",
            "metricnames": metric,
            "timespan": f"{date_str}T00:00:00Z/{next_day}T00:00:00Z",
            "interval": "P1D",
            "aggregation": "Average",
        }
        try:
            resp = self._get(url, params)
            for m in resp.get("value", []):
                for series in m.get("timeseries", []):
                    for pt in series.get("data", []):
                        v = pt.get("average")
                        if v is not None:
                            return float(v)
        except Exception as exc:
            logger.debug("⚠ Métrica %s indisponível para %s/%s: %s", metric, server, db, exc)
        return 0.0

    def get_db_metrics(
        self, sub: str, rg: str, server: str, db: str, date_str: str
    ) -> tuple[float, float]:
        """Return (cpu_avg, disk_avg) for the database on the target date."""
        cpu = self._get_metric(sub, rg, server, db, "cpu_percent", date_str)
        disk = self._get_metric(sub, rg, server, db, "storage_percent", date_str)
        return cpu, disk


# ---------------------------------------------------------------------------
# Per-pool share computation
# ---------------------------------------------------------------------------

def _compute_pool_shares(
    arm: _ArmClient,
    service_key: str,
    resource_id: str,
    pool_daily_cost: float,
    date_str: str,
) -> tuple[list[dict], list[dict]]:
    """
    Return (share_rows, detail_rows) for one elastic pool.
    Returns ([], []) if pool cannot be processed.
    """
    parts = _parse_pool_id(resource_id)
    if not parts:
        logger.warning("⚠ Não foi possível parsear ResourceId: %s", resource_id)
        return [], []

    sub, rg, server, pool = parts["sub"], parts["rg"], parts["server"], parts["pool"]
    logger.info("🔍 Pool: %s/%s  custo=%.4f", server, pool, pool_daily_cost)

    try:
        databases = arm.list_pool_databases(sub, rg, server, pool)
    except Exception as exc:
        logger.warning("⚠ Falha ao listar databases de %s/%s: %s", server, pool, exc)
        return [], []

    # Filter system databases
    user_dbs = [db for db in databases if db.get("name", "").lower() not in _SYSTEM_DBS]
    if not user_dbs:
        logger.warning("⚠ Nenhum database de usuário encontrado em %s/%s", server, pool)
        return [], []

    n_dbs = len(user_dbs)
    equal_weight = 1.0 / n_dbs  # fallback weight when all metrics are zero

    # Gather per-database data
    db_records: list[dict] = []
    for db_obj in user_dbs:
        db_name = db_obj.get("name", "")

        tags = db_obj.get("tags") or {}
        cliente = next(
            (str(v).strip().upper() for k, v in tags.items() if k.upper() == _TAG_KEY),
            None,
        )
        if cliente == _COMPARTILHADO:
            cliente = None

        cpu, disk = arm.get_db_metrics(sub, rg, server, db_name, date_str)
        score = cpu * _CPU_WEIGHT + disk * _DISK_WEIGHT
        logger.debug("DB %s → cliente=%s  cpu=%.4f  disk=%.4f  score=%.4f", db_name, cliente, cpu, disk, score)
        db_records.append({"name": db_name, "cliente": cliente, "cpu": cpu, "disk": disk, "score": score})

    total_score = sum(r["score"] for r in db_records)

    # Fallback: if all scores are zero, use equal weight (1/N)
    if total_score == 0.0:
        logger.warning("⚠ Todos os scores são 0 em %s/%s — usando peso igual (1/%d)", server, pool, n_dbs)
        for r in db_records:
            r["score"] = equal_weight
        total_score = 1.0

    # Group scores by client
    client_score: dict[str, float] = {}
    unmapped_score = 0.0
    for r in db_records:
        if r["cliente"]:
            client_score[r["cliente"]] = client_score.get(r["cliente"], 0.0) + r["score"]
        else:
            unmapped_score += r["score"]

    if not client_score:
        logger.warning("⚠ Nenhum database com tag FINOPS-CLIENTE em %s/%s — pool sem share", server, pool)
        return [], []

    # Redistribute unmapped proportionally (same pattern as s03b system namespaces)
    mapped_total = sum(client_score.values())
    redistrib_factor = 1.0 + (unmapped_score / mapped_total)
    client_score_adj = {c: v * redistrib_factor for c, v in client_score.items()}
    grand_total = sum(client_score_adj.values())

    # Build share rows
    client_shares = {
        c: round(s / grand_total, 6) for c, s in client_score_adj.items()
    }

    share_rows = [
        {
            "Date": date_str,
            "ServiceKey": service_key,
            "Cliente": cliente,
            "Share": share,
            "DriverType": "SQL_CPU_DISK_UTILIZATION",
            "AllocationMethod": "SQL_ELASTIC_POOL_CPU_DISK",
        }
        for cliente, share in client_shares.items()
    ]

    # Build detail rows for audit
    detail_rows = []
    for r in db_records:
        db_share = r["score"] / grand_total if grand_total > 0 else 0.0
        allocated = round(pool_daily_cost * db_share, 6)
        detail_rows.append({
            "Date": date_str,
            "ServiceKey": service_key,
            "PoolResourceId": resource_id,
            "Server": server,
            "Pool": pool,
            "DatabaseName": r["name"],
            "Cliente": r["cliente"] or "",
            "CpuAvg": round(r["cpu"], 6),
            "DiskAvg": round(r["disk"], 6),
            "Score": round(r["score"], 6),
            "Share": round(db_share, 6),
            "PoolDailyCost": round(pool_daily_cost, 6),
            "AllocatedCost": allocated,
        })

    share_sum = sum(r["Share"] for r in share_rows)
    logger.info(
        "✅ %s/%s → %d clientes | soma share=%.6f | custo=%.4f",
        server, pool, len(share_rows), share_sum, pool_daily_cost,
    )
    return share_rows, detail_rows


# ---------------------------------------------------------------------------
# Step entry point
# ---------------------------------------------------------------------------

def run(pipeline_date: Optional[str] = None) -> None:
    date_str = resolve_pipeline_date(pipeline_date)
    logger.info("📅 s03c_sql_pool  date=%s", date_str)

    store = pipeline_client()
    partition = to_partition(date_str)

    r2s = store.read_parquet(f"{_R2S_PREFIX}/{partition}/resource_to_service.parquet")

    # Only non-dedicated SQL Elastic Pool resources — dedicated ones handled by s03a
    mask = (
        r2s["ResourceType"].str.lower().str.strip() == _ELASTIC_POOL_TYPE
    ) & (
        r2s["ServiceType"] != "dedicated"
    )
    sql_pool = r2s[mask].copy()
    logger.info("📊 %d recursos Elastic Pool (não-dedicados) identificados", len(sql_pool))

    if sql_pool.empty:
        logger.info("ℹ️  Sem recursos SQL Elastic Pool — step ignorado")
        return

    # Load fact_cost to get PoolDailyCost per ResourceId
    fact_cost = store.read_parquet(f"{_FACT_COST_PREFIX}/{partition}/fact_cost.parquet")
    cost_by_rid = (
        fact_cost.groupby("ResourceId")["Cost"].sum().to_dict()
    )

    arm = _ArmClient()
    all_share_rows: list[dict] = []
    all_detail_rows: list[dict] = []

    for _, row in sql_pool.drop_duplicates(subset=["ServiceKey"]).iterrows():
        service_key = str(row["ServiceKey"])
        resource_id = str(row["ResourceId"])
        pool_daily_cost = float(cost_by_rid.get(resource_id, 0.0))

        share_rows, detail_rows = _compute_pool_shares(
            arm, service_key, resource_id, pool_daily_cost, date_str
        )
        all_share_rows.extend(share_rows)
        all_detail_rows.extend(detail_rows)

    if not all_share_rows:
        logger.warning("⚠ Nenhum share SQL Pool gerado — recursos cairão em client_weights no s05")
        return

    df_shares = pd.DataFrame(all_share_rows, columns=_SHARE_COLUMNS)
    df_detail = pd.DataFrame(all_detail_rows, columns=_DETAIL_COLUMNS)

    store.write_csv(f"{_SQL_POOL_OUT_PREFIX}/{partition}/sql_pool.csv", df_shares)
    store.write_csv(f"{_SQL_POOL_OUT_PREFIX}/{partition}/sqlpool_detail_all.csv", df_detail)

    logger.info(
        "✅ sql_pool: %d shares, %d linhas detalhe → %s",
        len(df_shares), len(df_detail), f"{_SQL_POOL_OUT_PREFIX}/{partition}/",
    )
