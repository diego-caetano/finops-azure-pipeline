"""
run_pipeline.py — Entrypoint do pipeline principal FinOps.

Executa steps em ordem: s01 → s02 → s03a → s03c → s04 → s05 → s06 → s07.
Roda às 06:00 UTC em aks-prd-newkmm (cluster central).
"""

import ctypes
import gc
import logging
import os
import sys

from finops.steps import (
    s01_ingest_focus,
    s02_classify,
    s03a_dedicated,
    s03c_sql_pool,
    s04_client_weights,
    s05_consolidate,
    s06_fact_allocated_cost,
    s07_consolidate_pbi,
)

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


def _rss_mb() -> float:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except Exception:
        pass
    return 0.0


def _release_memory() -> None:
    """Force Python GC + glibc page release so each step starts with a clean heap."""
    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


_STEPS = [
    ("s01_ingest_focus", s01_ingest_focus.run),
    ("s02_classify", s02_classify.run),
    ("s03a_dedicated", s03a_dedicated.run),
    ("s03c_sql_pool", s03c_sql_pool.run),
    ("s04_client_weights", s04_client_weights.run),
    ("s05_consolidate", s05_consolidate.run),
    ("s06_fact_allocated_cost", s06_fact_allocated_cost.run),
    ("s07_consolidate_pbi", s07_consolidate_pbi.run),
]


def main() -> None:
    pipeline_date = os.environ.get("PIPELINE_DATE") or None
    logger.info("🚀 finops-pipeline iniciado  date=%s", pipeline_date or "auto(D-3 BRT)")

    for name, step_fn in _STEPS:
        logger.info("▶️  %s  [RSS=%.0fMB]", name, _rss_mb())
        step_fn(pipeline_date)
        _release_memory()
        logger.info("✅ %s concluído  [RSS=%.0fMB]", name, _rss_mb())

    logger.info("🏁 finops-pipeline concluído")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("❌ finops-pipeline falhou")
        sys.exit(1)
