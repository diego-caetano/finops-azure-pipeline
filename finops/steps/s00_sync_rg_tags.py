"""
s00_sync_rg_tags.py — Sincronização de tags FINOPS-CLIENTE dos Resource Groups.

Varre as subscriptions configuradas em SYNC_SUBSCRIPTION_IDS, extrai a tag
FINOPS-CLIENTE de cada RG e grava config/reference/rg_tags.csv no pipeline
storage (sobrescreve a versão anterior).

Roda às 04:00 UTC (antes do pipeline principal às 06:00).

Permissão necessária na identity:
  Reader (ou pelo menos Microsoft.Resources/subscriptions/resourceGroups/read)
  em cada subscription listada em SYNC_SUBSCRIPTION_IDS.
"""

import logging
import os
from datetime import datetime, timezone

import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource.resources import ResourceManagementClient

from finops.storage import pipeline_client

logger = logging.getLogger(__name__)

_REFERENCE_PREFIX = os.getenv("REFERENCE_PREFIX", "config/reference")
_RG_TAGS_BLOB = f"{_REFERENCE_PREFIX}/rg_tags.csv"
_TAG_KEY = "FINOPS-CLIENTE"


def _get_subscriptions() -> list[str]:
    raw = os.environ.get("SYNC_SUBSCRIPTION_IDS", "")
    subs = [s.strip() for s in raw.split(",") if s.strip()]
    if not subs:
        raise ValueError("SYNC_SUBSCRIPTION_IDS não definido ou vazio.")
    return subs


def _scan_subscription(sub_id: str, credential: DefaultAzureCredential) -> list[dict]:
    client = ResourceManagementClient(credential, sub_id)
    rows: list[dict] = []
    updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    for rg in client.resource_groups.list():
        tags = rg.tags or {}
        # tag lookup case-insensitive
        finops_cliente = next(
            (v for k, v in tags.items() if k.upper() == _TAG_KEY),
            None,
        )
        if finops_cliente is None:
            continue
        rows.append(
            {
                "ResourceGroupName": rg.name,
                "FinopsCliente": finops_cliente.strip().upper(),
                "SubscriptionId": sub_id,
                "UpdatedAt": updated_at,
            }
        )

    logger.info("📋 sub=%s → %d RGs com tag %s", sub_id, len(rows), _TAG_KEY)
    return rows


def run() -> None:
    subscriptions = _get_subscriptions()
    logger.info("🔍 Varrendo %d subscription(s): %s", len(subscriptions), subscriptions)

    credential = DefaultAzureCredential()
    all_rows: list[dict] = []

    failed: list[str] = []
    for sub_id in subscriptions:
        try:
            rows = _scan_subscription(sub_id, credential)
            all_rows.extend(rows)
        except Exception as exc:
            logger.error("❌ Falha ao varrer sub=%s: %s", sub_id, exc)
            failed.append(sub_id)

    if not all_rows:
        logger.warning("⚠ Nenhum RG com tag %s encontrado — abortando gravação", _TAG_KEY)
        return

    if failed:
        logger.warning("⚠ %d subscription(s) com falha (sem permissão?): %s", len(failed), failed)
        logger.warning("⚠ Gravando resultado parcial com %d subscription(s) bem-sucedida(s)", len(subscriptions) - len(failed))

    df = pd.DataFrame(all_rows, columns=["ResourceGroupName", "FinopsCliente", "SubscriptionId", "UpdatedAt"])
    df = df.sort_values(["SubscriptionId", "ResourceGroupName"]).reset_index(drop=True)

    store = pipeline_client()
    store.write_csv(_RG_TAGS_BLOB, df)

    logger.info("✅ rg_tags.csv gravado: %d RGs em %d subscription(s)", len(df), len(subscriptions))
