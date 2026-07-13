"""
run_suggestions.py — Entrypoint do job semanal de sugestões de redução de custos.

Roda toda segunda-feira às 09:00 UTC em aks-prd-newkmm, após o finops-retro-fix (07:00 UTC).
Analisa os últimos 7 dias e grava sugestões em gold/cost_suggestions/.
"""

import logging
import os
import sys

from finops.steps import s09_cost_suggestions

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


def main() -> None:
    pipeline_date = os.environ.get("PIPELINE_DATE") or None
    logger.info("🚀 finops-suggestions iniciado  date=%s", pipeline_date or "auto(D-3 BRT)")
    s09_cost_suggestions.run(pipeline_date)
    logger.info("🏁 finops-suggestions concluído")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("❌ finops-suggestions falhou")
        sys.exit(1)
