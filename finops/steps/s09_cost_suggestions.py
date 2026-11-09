"""
s09_cost_suggestions.py — Sugestões semanais de redução de custos.

Analisa os últimos 7 dias de dados alocados e identifica oportunidades de
melhoria na alocação por cliente:

  1. PESO_PROPORCIONAL — recursos em WEIGHT_PROPORTIONAL com custo expressivo.
     Esses recursos não têm dono claro e o custo é dividido por todos os clientes.
     Ação: adicionar tag FINOPS-CLIENTE no recurso ou RG.

  2. RECURSO_SEM_DONO — recursos não alocados (unallocated_cost) com custo relevante.
     Motivos: NO_SERVICEKEY (sem ServiceKey) ou HAS_SERVICEKEY_NO_SHARE (sem share).
     Ação: adicionar tag FINOPS-CLIENTE ou mapear ServiceKey em s02.

Saída:
  gold/cost_suggestions/dt=yyyy-MM-dd/suggestions_{date}.csv

Prioridade:
  ALTA   → custo semanal >= R$500
  MEDIA  → custo semanal >= R$100
  BAIXA  → custo semanal >= R$20  (limiar mínimo configurável via MIN_SUGGESTION_COST)
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from finops.storage import pipeline_client, upload_csv
from finops.utils.dates import resolve_pipeline_date, to_partition

logger = logging.getLogger(__name__)

_FACT_PREFIX = os.getenv("FACT_ALLOCATED_COST_PREFIX", "gold/fact_allocated_cost")
_SUGGESTIONS_PREFIX = os.getenv("SUGGESTIONS_PREFIX", "gold/cost_suggestions")

_LOOKBACK_DAYS = 7
_MIN_COST = float(os.getenv("MIN_SUGGESTION_COST", "20.0"))

_ACTIONABLE_REASONS = {"NO_SERVICEKEY", "HAS_SERVICEKEY_NO_SHARE"}

_COL_ORDER = [
    "Category", "Prioridade", "WeeklyCost",
    "ResourceId", "ResourceName", "ResourceGroupName", "ResourceType", "SubscriptionId",
    "Reason", "Sugestao",
]


def _date_range(end_date_str: str, days: int) -> list[str]:
    end = datetime.strptime(end_date_str, "%Y-%m-%d")
    return [(end - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days - 1, -1, -1)]


def _priority(cost: float) -> str:
    if cost >= 500:
        return "ALTA"
    if cost >= 100:
        return "MEDIA"
    return "BAIXA"


def _read_fact_allocated(store, dates: list[str]) -> pd.DataFrame:
    """Lê fact_allocated_cost diário excluindo arquivos de unallocated."""
    frames = []
    for date in dates:
        blobs = store.list_blobs(f"{_FACT_PREFIX}/{to_partition(date)}/")
        for blob in blobs:
            if "unallocated" in blob.split("/")[-1]:
                continue
            try:
                if blob.endswith(".parquet"):
                    frames.append(store.read_parquet(blob))
                elif blob.endswith(".csv"):
                    frames.append(store.read_csv(blob))
            except Exception as exc:
                logger.warning("⚠ Erro ao ler %s: %s", blob, exc)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _read_unallocated(store, dates: list[str]) -> pd.DataFrame:
    """Lê unallocated_cost_{date}.csv dos últimos N dias."""
    frames = []
    for date in dates:
        blob = f"{_FACT_PREFIX}/{to_partition(date)}/unallocated_cost_{date}.csv"
        try:
            frames.append(store.read_csv(blob))
        except Exception:
            pass
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _aggregate_by_resource(df: pd.DataFrame, cost_col: str = "Cost") -> pd.DataFrame:
    """Deduplica por (Date, ResourceId) para remover fan-out e agrega custo semanal."""
    key = ["Date", "ResourceId"]
    meta = ["ResourceName", "ResourceGroupName", "ResourceType", "SubscriptionId"]
    cols = [c for c in key + meta + [cost_col] if c in df.columns]
    df_dedup = df[cols].drop_duplicates(subset=key)
    group = [c for c in ["ResourceId"] + meta if c in df_dedup.columns]
    return (
        df_dedup.groupby(group, as_index=False)[cost_col]
        .sum()
        .rename(columns={cost_col: "WeeklyCost"})
        .sort_values("WeeklyCost", ascending=False)
    )


def run(pipeline_date: Optional[str] = None) -> None:
    date_str = resolve_pipeline_date(pipeline_date)
    logger.info("📅 s09_cost_suggestions  date=%s  lookback=%d dias", date_str, _LOOKBACK_DAYS)

    store = pipeline_client()
    storage_account = os.environ["PIPELINE_STORAGE_ACCOUNT"]
    container = os.environ["PIPELINE_CONTAINER"]
    partition = to_partition(date_str)

    dates = _date_range(date_str, _LOOKBACK_DAYS)
    logger.info("📅 Período analisado: %s → %s", dates[0], dates[-1])

    suggestions: list[pd.DataFrame] = []

    # ── Cat. 1: PESO_PROPORCIONAL ─────────────────────────────────────────────
    df_fact = _read_fact_allocated(store, dates)

    if not df_fact.empty and "AllocationMethod" in df_fact.columns:
        df_wp = df_fact[df_fact["AllocationMethod"] == "WEIGHT_PROPORTIONAL"]
        if not df_wp.empty:
            df_agg = _aggregate_by_resource(df_wp)
            df_agg = df_agg[df_agg["WeeklyCost"] >= _MIN_COST]
            if not df_agg.empty:
                df_agg["Category"] = "PESO_PROPORCIONAL"
                df_agg["Sugestao"] = "Adicionar tag FINOPS-CLIENTE no recurso ou RG para alocação direta"
                df_agg["Reason"] = ""
                df_agg["Prioridade"] = df_agg["WeeklyCost"].apply(_priority)
                suggestions.append(df_agg)
                logger.warning(
                    "⚠ PESO_PROPORCIONAL: %d recursos sem dono claro | custo semanal=R$%.2f"
                    " | Ação: adicionar tag FINOPS-CLIENTE",
                    len(df_agg), float(df_agg["WeeklyCost"].sum()),
                )
    else:
        logger.warning("⚠ fact_allocated_cost não encontrado para o período — pulando Cat. 1")

    # ── Cat. 2: RECURSO_SEM_DONO (unallocated) ───────────────────────────────
    df_unalloc = _read_unallocated(store, dates)

    if not df_unalloc.empty and "Reason" in df_unalloc.columns:
        df_action = df_unalloc[df_unalloc["Reason"].isin(_ACTIONABLE_REASONS)].copy()
        if not df_action.empty:
            for reason, df_grp in df_action.groupby("Reason"):
                df_agg = _aggregate_by_resource(df_grp)
                df_agg = df_agg[df_agg["WeeklyCost"] >= _MIN_COST]
                if df_agg.empty:
                    continue
                df_agg["Category"] = "RECURSO_SEM_DONO"
                df_agg["Reason"] = reason
                df_agg["Sugestao"] = (
                    "Adicionar tag FINOPS-CLIENTE no recurso ou RG"
                    if reason == "NO_SERVICEKEY"
                    else "Mapear ServiceKey em s02 ou adicionar tag FINOPS-CLIENTE"
                )
                df_agg["Prioridade"] = df_agg["WeeklyCost"].apply(_priority)
                suggestions.append(df_agg)
            cost_action = float(df_action["Cost"].sum()) if "Cost" in df_action.columns else 0.0
            logger.warning(
                "⚠ RECURSO_SEM_DONO: %d recursos não alocados | custo semanal=R$%.2f"
                " | Motivos: %s | Ação: tagueamento ou mapeamento em s02",
                len(df_action["ResourceId"].unique()) if "ResourceId" in df_action.columns else 0,
                cost_action,
                df_action["Reason"].value_counts().to_dict(),
            )
    else:
        logger.info("✅ Nenhum recurso não alocado (actionable) no período")

    # ── Output ────────────────────────────────────────────────────────────────
    if not suggestions:
        logger.info(
            "✅ s09_cost_suggestions — nenhuma sugestão gerada para %s → %s",
            dates[0], dates[-1],
        )
        return

    df_out = pd.concat(suggestions, ignore_index=True)
    df_out = df_out.reindex(columns=[c for c in _COL_ORDER if c in df_out.columns])
    df_out = df_out.sort_values(
        ["Prioridade", "WeeklyCost"],
        ascending=[True, False],
        key=lambda s: s.map({"ALTA": 0, "MEDIA": 1, "BAIXA": 2}) if s.name == "Prioridade" else s,
    )

    blob_out = f"{_SUGGESTIONS_PREFIX}/{partition}/suggestions_{date_str}.csv"
    upload_csv(df_out, storage_account, container, blob_out)

    n_alta = int((df_out["Prioridade"] == "ALTA").sum())
    n_media = int((df_out["Prioridade"] == "MEDIA").sum())
    n_baixa = int((df_out["Prioridade"] == "BAIXA").sum())
    total = float(df_out["WeeklyCost"].sum())

    logger.warning(
        "⚠ s09 concluído: %d sugestões (ALTA=%d MEDIA=%d BAIXA=%d)"
        " | custo otimizável=R$%.2f/semana | %s",
        len(df_out), n_alta, n_media, n_baixa, total, blob_out,
    )
