"""
run_aks.py — Entry point do job AKS.

Executado como Pod dentro de cada cluster AKS compartilhado.
Lê CLUSTER_NAME e OPENCOST_ENDPOINT do ambiente (ConfigMap k8s)
e chama o step s03b_aks para a data alvo.
"""

import logging
import os
import sys

from finops.steps.s03b_aks import run
from finops.utils.dates import resolve_pipeline_date

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


def main() -> None:
    pipeline_date = os.environ.get("PIPELINE_DATE") or None
    date_str = resolve_pipeline_date(pipeline_date)
    cluster = os.environ.get("CLUSTER_NAME", "<desconhecido>")

    logger.info("🚀 Iniciando job AKS  cluster=%s  date=%s", cluster, date_str)
    run(pipeline_date=date_str)
    logger.info("✅ Job AKS concluído  cluster=%s  date=%s", cluster, date_str)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("❌ Job AKS falhou")
        sys.exit(1)
