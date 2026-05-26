"""
s03c_sql_pool.py — Alocação SQL Pool via métricas Azure Monitor
================================================================
Porta fiel do script 07 (PowerShell) para Python.

Lógica central:
  1. Lê resource_to_service para identificar ServiceKeys do tipo SQLPOOL:
  2. Para cada pool, lista os bancos de dados via az sql db list
  3. Coleta métricas de CPU% e Storage% via Azure Monitor (janela = UsageDate)
  4. Calcula share ponderado: WeightedUsage = (CPU/totalCPU)*CpuWeight + (Disk/totalDisk)*DiskWeight
  5. Share por cliente = WeightedUsage / totalWeightedUsage
  6. Fallback: se métricas = 0, divide igualmente por número de DBs

Pesos padrão (equivalente ao script 07 PS):
  CpuWeight  = 0.97
  DiskWeight = 0.03

Outputs (espelho exato dos CSVs do script 07):
  gold/sqlpool_allocation_share_metrics/dt=<date>/
    sqlpool_detail_all_<date>.csv
    sqlpool_allocation_share_all_<date>.csv
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta, timezone

import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.monitor.query import MetricsQueryClient, MetricAggregationType
from azure.mgmt.sql import SqlManagementClient

from finops.storage import download_csv_with_fallback, upload_csv
from finops.utils.dates import brazil_today, parse_usage_date, resolve_pipeline_date

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PIPELINE_STORAGE_ACCOUNT     = os.environ.get("PIPELINE_STORAGE_ACCOUNT", "stpslkmmfinopseusprd")
PIPELINE_CONTAINER           = os.environ.get("PIPELINE_CONTAINER", "finops-v2")
RESOURCE_TO_SERVICE_PREFIX   = os.environ.get("RESOURCE_TO_SERVICE_PREFIX", "silver/resource_to_service")
FACT_COST_PREFIX             = os.environ.get("FACT_COST_PREFIX", "silver/fact_cost")
OUT_PREFIX                   = os.environ.get("SQL_POOL_OUT_PREFIX", "gold/sql_pool_allocation_share")

CPU_WEIGHT  = float(os.environ.get("SQL_CPU_WEIGHT",  "0.97"))
DISK_WEIGHT = float(os.environ.get("SQL_DISK_WEIGHT", "0.03"))

_COMPARTILHADO = "COMPARTILHADO"

TARGET_SUBSCRIPTIONS = [
    s.strip()
    for s in os.environ.get(
        "SUBSCRIPTION_IDS",
        "52d4423b-7ed9-4673-b8e2-fa21cdb83176,3f6d197f-f70b-4c2c-b981-8bb575d47a7a"
    ).split(",")
    if s.strip()
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_float(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip().replace(",", "."))
    except (ValueError, TypeError):
        return 0.0


def _fmt6(value) -> str:
    return f"{_parse_float(value):.6f}"


def _parse_pool_from_resource_id(rid: str) -> dict | None:
    """
    Extrai SubscriptionId, ResourceGroup, Server e Pool do ResourceId.
    Equivalente a Get-PoolInfoFromResourceId do PS.
    """
    pattern = (
        r"^/subscriptions/(?P<sub>[^/]+)"
        r"/resourceGroups/(?P<rg>[^/]+)"
        r"/providers/Microsoft\.Sql/servers/(?P<server>[^/]+)"
        r"/elasticPools/(?P<pool>[^/]+)$"
    )
    m = re.match(pattern, rid.strip(), re.IGNORECASE)
    if not m:
        return None
    return {
        "SubscriptionId": m.group("sub"),
        "ResourceGroup":  m.group("rg"),
        "Server":         m.group("server"),
        "Pool":           m.group("pool"),
    }


def _build_day_window(usage_date: str) -> tuple[datetime, datetime]:
    """
    Retorna (start, end) UTC para a janela de métricas do dia.
    Equivalente a Build-DayWindowUtc do PS.
    """
    start = datetime.strptime(usage_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end   = start + timedelta(hours=23, minutes=59, seconds=59)
    return start, end


# ---------------------------------------------------------------------------
# Azure Monitor — métricas por banco de dados
# ---------------------------------------------------------------------------

def _get_metric_avg(
    metrics_client: MetricsQueryClient,
    resource_id: str,
    metric_name: str,
    start: datetime,
    end: datetime,
    precision: int = 4,
) -> float:
    """
    Retorna a média de uma métrica para um recurso no período.
    Equivalente a Get-MetricAvg do PS.
    """
    try:
        response = metrics_client.query_resource(
            resource_uri=resource_id,
            metric_names=[metric_name],
            timespan=(start, end),
            granularity=timedelta(hours=1),
            aggregations=[MetricAggregationType.AVERAGE],
        )
        values = []
        for metric in response.metrics:
            for ts in metric.timeseries:
                for dp in ts.data:
                    if dp.average is not None:
                        values.append(dp.average)

        if not values:
            return 0.0
        return round(sum(values) / len(values), precision)

    except Exception as exc:
        logger.warning(f"⚠ Métrica {metric_name} falhou para {resource_id}: {exc}")
        return 0.0


# ---------------------------------------------------------------------------
# Lista de bancos no pool — az sql db list equivalente
# ---------------------------------------------------------------------------

def _get_databases_in_pool(
    credential: DefaultAzureCredential,
    subscription_id: str,
    resource_group: str,
    server: str,
    pool: str,
) -> list[dict]:
    """
    Lista bancos de dados do pool com tag FINOPS-CLIENTE lida diretamente do ARM.
    Retorna list[{"name": str, "finops_cliente": str | None}].
    Tags são lidas do objeto db.tags (case-insensitive) — não requer $expand separado.
    """
    try:
        sql_client = SqlManagementClient(credential, subscription_id)
        dbs = sql_client.databases.list_by_server(resource_group, server)
        result = []
        for db in dbs:
            if db.elastic_pool_id and pool.lower() in db.elastic_pool_id.lower():
                tags = db.tags or {}
                tag_key = next((k for k in tags if k.lower() == "finops-cliente"), None)
                finops_cliente = tags[tag_key].strip().upper() if tag_key else None
                result.append({"name": db.name, "finops_cliente": finops_cliente})
        return sorted(result, key=lambda d: d["name"])
    except Exception as exc:
        logger.warning(
            f"⚠ Erro ao listar DBs do pool {pool} em {server}: {exc}"
        )
        return []


# ---------------------------------------------------------------------------
# Entrypoint principal
# ---------------------------------------------------------------------------

def run(pipeline_date: str | None = None) -> None:
    """
    Executa o step SQL Pool completo.
    Equivalente ao fluxo principal do script 07 PS.
    """
    pipeline_date = resolve_pipeline_date(pipeline_date)
    logger.info(f"🚀 s03c_sql_pool iniciado")
    logger.info(f"📅 Pipeline date: {pipeline_date}")
    logger.info(f"📅 Publish date : {brazil_today()}")
    logger.info(f"⚖  Pesos: CPU={CPU_WEIGHT} | Disk={DISK_WEIGHT}")

    credential = DefaultAzureCredential()

    # ── Downloads ─────────────────────────────────────────────────────────
    logger.info("\n== DOWNLOAD INPUTS ==")

    df_rts, _, _ = download_csv_with_fallback(
        PIPELINE_STORAGE_ACCOUNT, PIPELINE_CONTAINER,
        RESOURCE_TO_SERVICE_PREFIX, pipeline_date,
        r"resource_to_service.*\.csv$",
    )

    df_fact, _, _ = download_csv_with_fallback(
        PIPELINE_STORAGE_ACCOUNT, PIPELINE_CONTAINER,
        FACT_COST_PREFIX, pipeline_date,
        r"^fact_cost_.*\.csv$",
    )

    # ── Deriva UsageDate do conteúdo do fact_cost ──────────────────────────
    date_col = next(
        (c for c in ("Date", "UsageDate", "UsageDateTime") if c in df_fact.columns),
        None,
    )
    if date_col is None:
        raise ValueError("fact_cost sem coluna de data reconhecível")

    usage_date = (
        df_fact[date_col]
        .dropna()
        .apply(parse_usage_date)
        .dropna()
        .max()
    )
    logger.info(f"📅 UsageDate derivada: {usage_date}")

    # filtra fact_cost pela UsageDate
    df_fact = df_fact[
        df_fact[date_col].apply(parse_usage_date) == usage_date
    ].copy()

    # ── Índice custo diário por ServiceKey ─────────────────────────────────
    daily_cost_by_sk: dict[str, float] = {}
    for _, row in df_fact.iterrows():
        rid = str(row.get("ResourceId", "") or "").strip()
        if not rid:
            continue
        sk_rows = df_rts[
            df_rts["ResourceId"].astype(str).str.strip().str.lower() == rid.lower()
        ]
        if sk_rows.empty:
            continue
        sk = str(sk_rows.iloc[0]["ServiceKey"]).strip()
        cost = _parse_float(row.get("Cost", 0))
        daily_cost_by_sk[sk] = daily_cost_by_sk.get(sk, 0.0) + cost

    # ── Identifica pools SQLPOOL no resource_to_service ───────────────────
    logger.info("\n== DESCOBRINDO SQL POOLS ==")
    pools_seen: set[str] = set()
    pools: list[dict] = []

    # Filtra por ServiceKey=SQLPOOL: (definido em s02 para elastic pools, display name ou ARM format)
    sql_rows = df_rts[
        df_rts["ServiceKey"].astype(str).str.upper().str.startswith("SQLPOOL:")
    ]

    for _, row in sql_rows.iterrows():
        rid = str(row.get("ResourceId", "")).strip()
        sk  = str(row.get("ServiceKey", "")).strip()
        info = _parse_pool_from_resource_id(rid)
        if not info:
            continue
        key = f"{info['SubscriptionId']}|{info['ResourceGroup']}|{info['Server']}|{info['Pool']}"
        if key in pools_seen:
            continue
        pools_seen.add(key)
        pools.append({**info, "ServiceKey": sk})

    if not pools:
        logger.info("ℹ️  Nenhum SQLPOOL encontrado no resource_to_service — nada a processar para esta data")
        return

    logger.info(f"Pools encontrados: {len(pools)}")
    for p in pools:
        logger.info(f"  {p['ServiceKey']} | {p['Server']}/{p['Pool']}")

    # ── Clientes por DB — da tag FINOPS-CLIENTE no resource_to_service ────
    # Usa EffectiveFinopsCliente se disponível, senão FinopsClienteSource
    cliente_col = next(
        (c for c in ("EffectiveFinopsCliente", "FinopsCliente") if c in df_rts.columns),
        None,
    )

    # ── Métricas e cálculo de share ────────────────────────────────────────
    logger.info("\n== PROCESSANDO POOLS ==")
    metrics_client = MetricsQueryClient(credential)

    start_dt, end_dt = _build_day_window(usage_date)
    cpu_pct_label  = int(CPU_WEIGHT * 100)
    disk_pct_label = int(DISK_WEIGHT * 100)
    notes_label    = f"CPU{cpu_pct_label}_DISK{disk_pct_label}"

    all_detail: list[dict]   = []
    all_share:  list[dict]   = []
    all_diag:   list[dict]   = []
    sem_tag_rows: list[dict] = []

    for pool in pools:
        sub   = pool["SubscriptionId"]
        rg    = pool["ResourceGroup"]
        server = pool["Server"]
        pool_name = pool["Pool"]
        sk    = pool["ServiceKey"]

        logger.info(f"\n{'='*60}")
        logger.info(f"ServiceKey : {sk}")
        logger.info(f"Pool       : {pool_name} | Server: {server}")
        logger.info(f"Sub        : {sub}")

        pool_daily_cost = daily_cost_by_sk.get(sk, 0.0)
        logger.info(f"DailyCost  : {pool_daily_cost:.6f}")

        if pool_daily_cost <= 0:
            logger.warning(f"⚠ Sem custo para {sk} na data {usage_date}")
            all_diag.append({
                "ServiceKey": sk, "Status": "SKIPPED_NO_DAILY_COST",
                "PoolDailyCost": 0, "DbCount": 0,
                "TotalCpu": 0, "TotalDiskPct": 0, "TotalWeighted": 0,
            })
            continue

        # Lista bancos no pool com tags ARM
        db_names = _get_databases_in_pool(credential, sub, rg, server, pool_name)
        logger.info(f"DBs no pool: {len(db_names)}")
        for db_info in db_names:
            logger.info(
                "  🔍 Database: %s | Tag FINOPS-CLIENTE: %s",
                db_info["name"], db_info["finops_cliente"] or "AUSENTE",
            )

        if not db_names:
            logger.warning(f"⚠ Nenhuma DB encontrada no pool {pool_name}")
            all_diag.append({
                "ServiceKey": sk, "Status": "SKIPPED_NO_DATABASES",
                "PoolDailyCost": pool_daily_cost, "DbCount": 0,
                "TotalCpu": 0, "TotalDiskPct": 0, "TotalWeighted": 0,
            })
            continue

        # Coleta métricas por banco
        detail_rows: list[dict] = []
        for db_info in db_names:
            db = db_info["name"]
            db_rid = (
                f"/subscriptions/{sub}/resourceGroups/{rg}"
                f"/providers/Microsoft.Sql/servers/{server}/databases/{db}"
            )

            # 1ª opção: tag FINOPS-CLIENTE lida do ARM (case-insensitive, já normalizada)
            # 2ª opção: fallback via resource_to_service (databases raramente aparecem no FOCUS)
            cliente = _COMPARTILHADO
            finops_tag = db_info.get("finops_cliente")
            if finops_tag and finops_tag not in (_COMPARTILHADO, "SEM TAG", ""):
                cliente = finops_tag
            elif cliente_col:
                match = df_rts[
                    df_rts["ResourceId"].astype(str).str.strip().str.lower()
                    == db_rid.lower()
                ]
                if not match.empty:
                    val = str(match.iloc[0].get(cliente_col, "") or "").strip().upper()
                    if val and val not in (_COMPARTILHADO, "SEM TAG", ""):
                        cliente = val

            cpu_avg    = _get_metric_avg(metrics_client, db_rid, "cpu_percent",     start_dt, end_dt, 4)
            disk_pct   = _get_metric_avg(metrics_client, db_rid, "storage_percent", start_dt, end_dt, 4)
            disk_bytes = _get_metric_avg(metrics_client, db_rid, "storage",         start_dt, end_dt, 0)
            disk_gb    = round(disk_bytes / (1024**3), 4)

            logger.info(
                "  DB=%s | Cliente=%s | CPU=%.4f | DiskPct=%.4f | DiskGB=%.4f",
                db, cliente, cpu_avg, disk_pct, disk_gb,
            )

            if cliente == _COMPARTILHADO:
                logger.warning(
                    "⚠ Database sem tag FINOPS-CLIENTE — alocado como COMPARTILHADO: "
                    "Pool: %s | Database: %s | CPU: %.2f%%",
                    pool_name, db, cpu_avg,
                )
                sem_tag_rows.append({
                    "Date": usage_date, "Pool": pool_name, "Database": db,
                    "ResourceGroup": rg, "CPU_Percent": cpu_avg,
                    "AllocatedAs": _COMPARTILHADO, "Motivo": "Tag FINOPS-CLIENTE ausente",
                })

            detail_rows.append({
                "UsageDate": usage_date, "ServiceKey": sk,
                "Pool": pool_name, "SubscriptionId": sub,
                "ResourceGroup": rg, "Server": server,
                "Cliente": cliente, "Database": db, "ResourceId": db_rid,
                "CpuAvgPct": cpu_avg, "DiskPercentAvg": disk_pct,
                "DiskGBAvg": disk_gb,
            })

        if not detail_rows:
            all_diag.append({
                "ServiceKey": sk, "Status": "SKIPPED_NO_DETAIL",
                "PoolDailyCost": pool_daily_cost, "DbCount": 0,
                "TotalCpu": 0, "TotalDiskPct": 0, "TotalWeighted": 0,
            })
            continue

        total_cpu  = sum(r["CpuAvgPct"]      for r in detail_rows)
        total_disk = sum(r["DiskPercentAvg"] for r in detail_rows)
        logger.info(f"TotalCpu={total_cpu:.4f} | TotalDisk={total_disk:.4f}")

        # Calcula WeightedUsage por banco
        for r in detail_rows:
            contrib_cpu  = (r["CpuAvgPct"]      / total_cpu  * 100) if total_cpu  > 0 else 0.0
            contrib_disk = (r["DiskPercentAvg"] / total_disk * 100) if total_disk > 0 else 0.0
            weighted = (contrib_cpu * CPU_WEIGHT) + (contrib_disk * DISK_WEIGHT)
            r["ContribCpuPct"]  = round(contrib_cpu, 4)
            r["ContribDiskPct"] = round(contrib_disk, 4)
            r["WeightedUsage"]  = round(weighted, 6)

        total_weighted = sum(r["WeightedUsage"] for r in detail_rows)
        logger.info("TotalWeighted=%.6f", total_weighted)

        # Separa tagueados de COMPARTILHADO — redistribui custo de COMPARTILHADO aos tagueados
        tagged_rows        = [r for r in detail_rows if r["Cliente"] != _COMPARTILHADO]
        compartilhado_rows = [r for r in detail_rows if r["Cliente"] == _COMPARTILHADO]

        if not tagged_rows:
            # Todos sem tag — fallback: distribui igualmente entre todos
            logger.warning("⚠ Todos os databases sem tag em %s — fallback igual por %d DBs", sk, len(detail_rows))
            tagged_rows = detail_rows
            compartilhado_rows = []

        if compartilhado_rows:
            comp_weight = sum(r["WeightedUsage"] for r in compartilhado_rows)
            logger.info(
                "  Redistribuindo %d DB(s) COMPARTILHADO (WeightedUsage=%.6f) para %d cliente(s) tagueado(s)",
                len(compartilhado_rows), comp_weight, len(tagged_rows),
            )

        total_tagged_weighted = sum(r["WeightedUsage"] for r in tagged_rows)

        # Share — fallback igual por DB se weighted = 0; COMPARTILHADO absorvido pelos tagueados
        fallback = False
        if total_tagged_weighted <= 0:
            logger.warning("⚠ TotalTaggedWeighted=0 → fallback igual por %d DBs", len(tagged_rows))
            fallback = True
            equal_share = 1.0 / len(tagged_rows)

        for r in tagged_rows:
            if fallback:
                share = equal_share
                driver = "SQLPOOL_DB_COUNT_FALLBACK"
                allocation_method = "SQLPOOL_DB_COUNT"
            else:
                share = r["WeightedUsage"] / total_tagged_weighted
                driver = "SQLPOOL_DB_METRICS"
                allocation_method = "SQLPOOL_CPU"

            alloc_cost = pool_daily_cost * share

            r["Share"]          = round(share, 6)
            r["AllocatedCost"]  = round(alloc_cost, 6)
            r["PoolDailyCost"]  = pool_daily_cost
            r["DriverFallback"] = "DB_COUNT_EQUAL_SPLIT" if fallback else ""

            all_detail.append(r)
            all_share.append({
                "Date":             usage_date,
                "ServiceKey":       sk,
                "Cliente":          r["Cliente"],
                "Share":            round(share, 6),
                "DriverType":       driver,
                "AllocationMethod": allocation_method,
            })

        # COMPARTILHADO vai apenas para auditoria (share=0, custo redistribuído para tagueados)
        for r in compartilhado_rows:
            r["Share"]          = 0.0
            r["AllocatedCost"]  = 0.0
            r["PoolDailyCost"]  = pool_daily_cost
            r["DriverFallback"] = "REDISTRIBUTED_TO_TAGGED_CLIENTS"
            all_detail.append(r)

        status = "PROCESSED_FALLBACK_DB_COUNT" if fallback else "PROCESSED_METRICS"
        all_diag.append({
            "ServiceKey": sk, "Status": status,
            "PoolDailyCost": pool_daily_cost,
            "DbCount": len(detail_rows),
            "TotalCpu": total_cpu,
            "TotalDiskPct": total_disk,
            "TotalWeighted": total_weighted,
        })
        logger.info(
            "✅ %s — %d DB(s) tagueado(s) + %d COMPARTILHADO redistribuído(s)",
            sk, len(tagged_rows), len(compartilhado_rows),
        )

    if not all_detail:
        logger.warning("⚠ Nenhum pool SQL processado com sucesso — pools encontrados mas sem DBs acessíveis")
        return

    # ── Agrega shares por (Date, ServiceKey, Cliente) ──────────────────────
    df_share_raw = pd.DataFrame(all_share)
    df_share = (
        df_share_raw
        .groupby(["Date", "ServiceKey", "Cliente", "DriverType", "AllocationMethod"], as_index=False)
        .agg(Share=("Share", "sum"))
    )

    # Renormaliza por ServiceKey para garantir soma = 1.0
    for sk_val, grp in df_share.groupby("ServiceKey"):
        total = grp["Share"].sum()
        if abs(total - 1.0) > 1e-6 and total > 0:
            logger.info(f"  Renormalizando {sk_val}: soma={total:.8f}")
            df_share.loc[grp.index, "Share"] = (grp["Share"] / total).round(6)

    # ── Upload ─────────────────────────────────────────────────────────────
    logger.info("\n== UPLOAD ==")
    base = f"{OUT_PREFIX}/dt={usage_date}"

    df_detail_out = pd.DataFrame(all_detail)
    # formata decimais
    for col in ("CpuAvgPct", "DiskPercentAvg", "DiskGBAvg",
                "ContribCpuPct", "ContribDiskPct", "WeightedUsage",
                "Share", "AllocatedCost", "PoolDailyCost"):
        if col in df_detail_out.columns:
            df_detail_out[col] = df_detail_out[col].apply(_fmt6)

    df_share["Share"] = df_share["Share"].apply(_fmt6)

    upload_csv(
        df_detail_out,
        PIPELINE_STORAGE_ACCOUNT, PIPELINE_CONTAINER,
        f"{base}/sqlpool_detail_all_{usage_date}.csv",
    )
    upload_csv(
        df_share,
        PIPELINE_STORAGE_ACCOUNT, PIPELINE_CONTAINER,
        f"{base}/sqlpool_allocation_share_all_{usage_date}.csv",
    )

    if sem_tag_rows:
        df_sem_tag = pd.DataFrame(sem_tag_rows)
        upload_csv(
            df_sem_tag,
            PIPELINE_STORAGE_ACCOUNT, PIPELINE_CONTAINER,
            f"{base}/databases_sem_tag_{usage_date}.csv",
        )
        logger.info(
            "📋 Databases sem tag: %d em %d pools",
            len(sem_tag_rows), df_sem_tag["Pool"].nunique(),
        )

    # ── Resumo ─────────────────────────────────────────────────────────────
    logger.info("\n== RESUMO ==")
    df_diag = pd.DataFrame(all_diag)
    for status, grp in df_diag.groupby("Status"):
        logger.info(f"  {status}: {len(grp)} pool(s)")
    logger.info(f"  Total share rows: {len(df_share)}")
    logger.info(f"\n✅ s03c_sql_pool concluído")