"""
s04_client_weights.py — Peso por cliente via custo dedicado.

Calcula o peso financeiro de cada cliente somando os custos de recursos
classificados como dedicados (TAG_RESOURCE / TAG_RG_INHERITED). Esse peso é
usado por s05 para distribuir os custos de recursos compartilhados.

Fallback: se não houver custo dedicado, distribui igualmente entre todos os
clientes conhecidos (rg_tags.csv + clientes_alias.csv).
"""

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
_CLIENT_WEIGHTS_PREFIX = os.getenv("CLIENT_WEIGHTS_PREFIX", "silver/client_weights")

_OUTPUT_COLUMNS = ["Date", "Cliente", "DedicatedCost", "TotalDedicatedCost", "Weight"]


def _known_clients(store) -> list[str]:
    """Collect all non-COMPARTILHADO clients from reference files."""
    clients: set[str] = set()
    try:
        rg_df = store.read_csv(f"{_REFERENCE_PREFIX}/rg_tags.csv")
        for val in rg_df["FinopsCliente"].dropna().unique():
            v = str(val).strip().upper()
            if v and v != "COMPARTILHADO":
                clients.add(v)
    except Exception:
        pass
    try:
        alias_df = store.read_csv(f"{_REFERENCE_PREFIX}/clientes_alias.csv")
        for val in alias_df["Cliente"].dropna().unique():
            v = str(val).strip().upper()
            if v and v != "COMPARTILHADO":
                clients.add(v)
    except Exception:
        pass
    return sorted(clients)


def run(pipeline_date: Optional[str] = None) -> None:
    date_str = resolve_pipeline_date(pipeline_date)
    logger.info("📅 s04_client_weights  date=%s", date_str)

    store = pipeline_client()
    partition = to_partition(date_str)

    df_fc = store.read_parquet(f"{_FACT_COST_PREFIX}/{partition}/fact_cost.parquet")
    df_r2s = store.read_parquet(f"{_R2S_PREFIX}/{partition}/resource_to_service.parquet")

    dedicated = df_r2s[df_r2s["ServiceType"] == "dedicated"][
        ["ResourceId", "EffectiveFinopsCliente"]
    ]

    merged = df_fc[["ResourceId", "Cost"]].merge(dedicated, on="ResourceId", how="inner")
    client_costs = merged.groupby("EffectiveFinopsCliente")["Cost"].sum()
    total_dedicated = float(client_costs.sum())

    if total_dedicated > 0:
        weights = (client_costs / total_dedicated).to_dict()
        logger.info("📊 Custo dedicado total=%.2f, %d clientes", total_dedicated, len(weights))
    else:
        logger.warning("⚠ Sem custo dedicado — usando peso igualitário entre clientes conhecidos")
        clients = _known_clients(store)
        if not clients:
            raise ValueError(
                "Nenhum cliente conhecido para peso igualitário — "
                "verifique rg_tags.csv e clientes_alias.csv"
            )
        eq_weight = round(1.0 / len(clients), 6)
        weights = {c: eq_weight for c in clients}
        client_costs = pd.Series(dtype=float)
        total_dedicated = 0.0

    rows = [
        {
            "Date": date_str,
            "Cliente": cliente,
            "DedicatedCost": round(float(client_costs.get(cliente, 0.0)), 6),
            "TotalDedicatedCost": round(total_dedicated, 6),
            "Weight": round(float(w), 6),
        }
        for cliente, w in sorted(weights.items())
    ]

    df_out = pd.DataFrame(rows, columns=_OUTPUT_COLUMNS)
    logger.info("✅ client_weights: %d clientes, soma=%.6f", len(df_out), df_out["Weight"].sum())

    blob_name = f"{_CLIENT_WEIGHTS_PREFIX}/{partition}/client_weights.csv"
    store.write_csv(blob_name, df_out)
    logger.info("⬆️  Gravado: %s", blob_name)
