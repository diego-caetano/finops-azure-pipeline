"""
run_sync_rg_tags.py — Entrypoint do job de sincronização de tags de RG.

Roda às 04:00 UTC em um único cluster central (aks-prd-newkmm).
A identity deste cluster precisa de Reader nas subscriptions configuradas
em SYNC_SUBSCRIPTION_IDS, além de Storage Blob Data Contributor em finops-v2.
"""

import logging
import os
import sys

from finops.steps.s00_sync_rg_tags import run

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("🚀 Iniciando sync rg_tags")
    run()
    logger.info("✅ Sync rg_tags concluído")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("❌ Sync rg_tags falhou")
        sys.exit(1)
