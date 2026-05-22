#!/usr/bin/env python3
"""Retrocarga corretiva — reprocessa datas com FOCUS billing lag.

Uso:
  python sh/retro_fix.py                           # últimos 7 dias (padrão CronJob)
  python sh/retro_fix.py --start 2026-05-15 --end 2026-05-20

Pode ser executado:
  - Localmente (requer az login com permissões no blob)
  - Como CronJob Kubernetes (usa Workload Identity automaticamente)

Executa os mesmos steps do pipeline principal (s01→s07) para cada data,
lendo o FOCUS mais recente do blob. Cobre billing lag de 2–3 dias e casos
extremos como RI, Savings Plans e virada de mês.
"""

import argparse
import ctypes
import gc
import io
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

BRT = timezone(timedelta(hours=-3))
DEFAULT_LOOKBACK_DAYS = 7


def _load_dotenv() -> None:
    env_file = _ROOT / ".env"
    if not env_file.exists():
        return
    with open(env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())


_load_dotenv()
os.environ.setdefault("PIPELINE_RUN_ID", "retro-fix")

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

from finops.storage import pipeline_client  # noqa: E402
from finops.steps import (  # noqa: E402
    s01_ingest_focus,
    s02_classify,
    s03a_dedicated,
    s03c_sql_pool,
    s04_client_weights,
    s05_consolidate,
    s06_fact_allocated_cost,
    s07_consolidate_pbi,
)

_STEPS = [
    ("s01_ingest_focus",      s01_ingest_focus.run),
    ("s02_classify",          s02_classify.run),
    ("s03a_dedicated",        s03a_dedicated.run),
    ("s03c_sql_pool",         s03c_sql_pool.run),
    ("s04_client_weights",    s04_client_weights.run),
    ("s05_consolidate",       s05_consolidate.run),
    ("s06_fact_allocated_cost", s06_fact_allocated_cost.run),
    ("s07_consolidate_pbi",   s07_consolidate_pbi.run),
]


def _release_memory() -> None:
    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


def parse_args() -> tuple[str, str]:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--start", help="Data inicial YYYY-MM-DD (inclusive)")
    parser.add_argument("--end",   help="Data final   YYYY-MM-DD (inclusive)")
    args = parser.parse_args()

    if bool(args.start) != bool(args.end):
        parser.error("--start e --end devem ser usados juntos")

    if args.start and args.end:
        return args.start, args.end

    today = datetime.now(BRT)
    end   = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    start = (today - timedelta(days=DEFAULT_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    return start, end


def date_range(start: str, end: str) -> list[str]:
    s, e, dates = (
        datetime.strptime(start, "%Y-%m-%d"),
        datetime.strptime(end,   "%Y-%m-%d"),
        [],
    )
    cur = s
    while cur <= e:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return dates


def _read_manifest_antes(store, dates: list[str]) -> dict[str, float]:
    """Lê TotalAllocated do manifest atual para as datas do lote."""
    before: dict[str, float] = {}
    try:
        df = store.read_csv("gold/fact_allocated_cost_consolidated/manifest.csv")
        for _, row in df.iterrows():
            d = str(row.get("Date", "")).strip()
            if d in dates:
                before[d] = float(row.get("TotalAllocated", 0))
    except Exception:
        logger.warning("⚠ Manifest não encontrado — valores 'antes' serão 0")
    return before


def _read_total_fact_cost(store, date_str: str) -> float:
    """Lê TotalFactCost da reconciliation gerada pelo s06."""
    try:
        df = store.read_csv(
            f"gold/fact_allocated_cost/dt={date_str}/reconciliation_{date_str}.csv"
        )
        return float(df["TotalFactCost"].iloc[0])
    except Exception:
        return 0.0


def run_date(date_str: str) -> None:
    for name, step_fn in _STEPS:
        logger.info("▶️  %s", name)
        step_fn(date_str)
        _release_memory()
        logger.info("✅ %s concluído", name)


def main() -> None:
    start, end = parse_args()
    dates = date_range(start, end)

    logger.info("🔄 retro-fix: %s → %s (%d datas)", start, end, len(dates))
    logger.info("   Motivo: FOCUS billing lag — reprocessamento com dados settlados")

    store = pipeline_client()

    logger.info("📋 Lendo manifest atual (valores antes)...")
    before_map = _read_manifest_antes(store, dates)

    pass_count  = 0
    fail_count  = 0
    total_antes  = 0.0
    total_depois = 0.0

    for date_str in dates:
        logger.info("=" * 54)
        logger.info(">> %s", date_str)
        antes = before_map.get(date_str, 0.0)
        depois = 0.0
        try:
            run_date(date_str)
            depois = _read_total_fact_cost(store, date_str)
            diff_pct = ((depois - antes) / antes * 100) if antes > 0 else 0.0
            sign = "+" if diff_pct >= 0 else ""
            logger.info(
                "✅ %s | antes=%,.2f  depois=%,.2f  (%s%.1f%%)",
                date_str, antes, depois, sign, diff_pct,
            )
            pass_count  += 1
            total_antes  += antes
            total_depois += depois
        except Exception:
            logger.exception("❌ Falha em %s", date_str)
            fail_count += 1

    # Resumo final
    logger.info("=" * 54)
    logger.info("📊 Resumo retro-fix: %s → %s", start, end)
    logger.info("   Datas OK  : %d/%d", pass_count, len(dates))
    if fail_count:
        logger.warning("   Falhas    : %d", fail_count)
    if pass_count > 0:
        diff_total = total_depois - total_antes
        diff_pct   = (diff_total / total_antes * 100) if total_antes > 0 else 0.0
        sign = "+" if diff_total >= 0 else ""
        logger.info("   Total antes : %,.2f", total_antes)
        logger.info("   Total depois: %,.2f", total_depois)
        logger.info("   Diferença   : %s%,.2f (%s%.1f%%)", sign, diff_total, sign, diff_pct)

    sys.exit(0 if fail_count == 0 else 1)


if __name__ == "__main__":
    main()
