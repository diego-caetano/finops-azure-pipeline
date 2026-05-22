import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

BRT = timezone(timedelta(hours=-3))

# Billing lag confirmado em produção: charges de um dia continuam aparecendo
# no FOCUS por 2–3 dias após a data de uso. Lags maiores (RI, Savings Plans,
# virada de mês) são cobertos pelo CronJob finops-retro-fix toda segunda-feira.
FOCUS_LAG_DAYS = 3


def today_brt() -> datetime:
    return datetime.now(BRT)


def brazil_today() -> str:
    """Today in BRT as YYYY-MM-DD string."""
    return today_brt().strftime("%Y-%m-%d")


def parse_usage_date(val) -> Optional[str]:
    """Convert a date value (str, datetime, Timestamp, date) to YYYY-MM-DD string.

    Returns None if parsing fails.
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        if isinstance(val, str):
            return datetime.strptime(val[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
        if hasattr(val, "strftime"):
            return val.strftime("%Y-%m-%d")
        return None
    except (ValueError, TypeError):
        return None


def resolve_pipeline_date(pipeline_date: str | None = None) -> str:
    """Return target date as YYYY-MM-DD.

    If pipeline_date is provided (retrocarga manual), validate format and return it.
    Otherwise return today BRT - FOCUS_LAG_DAYS to ensure charges are settled in FOCUS.
    """
    if pipeline_date:
        datetime.strptime(pipeline_date, "%Y-%m-%d")  # validates format
        logger.info("📅 Pipeline date: %s (manual/retrocarga)", pipeline_date)
        return pipeline_date
    auto = today_brt() - timedelta(days=FOCUS_LAG_DAYS)
    date_str = auto.strftime("%Y-%m-%d")
    logger.info("📅 Pipeline date: %s (D-%d, FOCUS lag=%d dias)", date_str, FOCUS_LAG_DAYS, FOCUS_LAG_DAYS)
    return date_str


def to_partition(date_str: str) -> str:
    """Convert YYYY-MM-DD to blob partition prefix dt=YYYY-MM-DD."""
    return f"dt={date_str}"
