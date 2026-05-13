"""
s03a_dedicated.py — Recursos dedicados → share=1.0 por cliente.

Lê resource_to_service onde ServiceType=dedicated e gera shares com Share=1.0,
destinando 100% do custo de cada recurso ao cliente identificado em s02.
"""

import logging
import os
from typing import Optional

import pandas as pd

from finops.storage import pipeline_client
from finops.utils.dates import resolve_pipeline_date, to_partition

logger = logging.getLogger(__name__)

_R2S_PREFIX = os.getenv("RESOURCE_TO_SERVICE_PREFIX", "silver/resource_to_service")
_DEDICATED_OUT_PREFIX = os.getenv("DEDICATED_OUT_PREFIX", "gold/dedicated_allocation_share")

_OUTPUT_COLUMNS = ["Date", "ServiceKey", "Cliente", "Share", "DriverType", "AllocationMethod"]


def run(pipeline_date: Optional[str] = None) -> None:
    date_str = resolve_pipeline_date(pipeline_date)
    logger.info("📅 s03a_dedicated  date=%s", date_str)

    store = pipeline_client()

    r2s = store.read_parquet(
        f"{_R2S_PREFIX}/{to_partition(date_str)}/resource_to_service.parquet"
    )

    # Deduplicate by (ServiceKey, EffectiveFinopsCliente): fact_cost may have multiple
    # charge lines per resource (e.g. reservation + actual usage) producing duplicate
    # rows in r2s with the same ServiceKey. Without dedup, s05 would sum Share > 1.0.
    dedicated = (
        r2s[r2s["ServiceType"] == "dedicated"][["ServiceKey", "EffectiveFinopsCliente"]]
        .drop_duplicates()
    )
    logger.info("📊 %d recursos dedicados únicos", len(dedicated))

    if dedicated.empty:
        logger.warning("⚠ Nenhum recurso dedicado — arquivo de share não gravado")
        return

    df_out = pd.DataFrame({
        "Date": date_str,
        "ServiceKey": dedicated["ServiceKey"].values,
        "Cliente": dedicated["EffectiveFinopsCliente"].values,
        "Share": 1.0,
        "DriverType": "DEDICATED",
        "AllocationMethod": "DEDICATED_TAG",
    })[_OUTPUT_COLUMNS]

    blob_name = f"{_DEDICATED_OUT_PREFIX}/{to_partition(date_str)}/dedicated.csv"
    store.write_csv(blob_name, df_out)
    logger.info("✅ dedicated_share: %d linhas → %s", len(df_out), blob_name)
