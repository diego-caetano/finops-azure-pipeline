from datetime import datetime, timedelta, timezone

BRT = timezone(timedelta(hours=-3))


def today_brt() -> datetime:
    return datetime.now(BRT)


def resolve_pipeline_date(pipeline_date: str | None = None) -> str:
    """Return target date as YYYY-MM-DD.

    If pipeline_date is provided, validate format and return it.
    Otherwise return D-1 in BRT (UTC-3, no DST).
    """
    if pipeline_date:
        datetime.strptime(pipeline_date, "%Y-%m-%d")  # validates format
        return pipeline_date
    d1 = today_brt() - timedelta(days=1)
    return d1.strftime("%Y-%m-%d")


def to_partition(date_str: str) -> str:
    """Convert YYYY-MM-DD to blob partition prefix dt=YYYY-MM-DD."""
    return f"dt={date_str}"
