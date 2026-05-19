#!/usr/bin/env python3
"""
build_consolidated.py — Constrói o consolidado Power BI a partir do histórico.

Lê todas as partições existentes em gold/fact_allocated_cost/dt=*/
e chama s07_consolidate_pbi.run() para cada data em ordem crescente.

Uso:
    python sh/build_consolidated.py

Requer variáveis de ambiente do pipeline (ou arquivo .env na raiz do projeto).
"""

import io
import logging
import os
import re
import sys
from pathlib import Path

# Force UTF-8 output
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Garante que o root do projeto está no sys.path
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))


def _load_dotenv() -> None:
    """Carrega .env da raiz do projeto se existir (sem dependência extra)."""
    env_file = _ROOT / ".env"
    if not env_file.exists():
        return
    with open(env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())
    print(f"[dotenv] carregado: {env_file}")


_load_dotenv()

# Identifica esta execução no manifest
os.environ.setdefault("PIPELINE_RUN_ID", "build_consolidated")

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)

from finops.storage import pipeline_client  # noqa: E402
from finops.steps import s07_consolidate_pbi  # noqa: E402

_FACT_PREFIX = os.getenv("FACT_ALLOCATED_COST_PREFIX", "gold/fact_allocated_cost")


def main() -> None:
    store = pipeline_client()

    blobs = store.list_blobs(f"{_FACT_PREFIX}/")
    dates_found: set[str] = set()
    for b in blobs:
        m = re.search(
            r"dt=(\d{4}-\d{2}-\d{2})/fact_allocated_cost_\d{4}-\d{2}-\d{2}\.parquet$", b
        )
        if m:
            dates_found.add(m.group(1))

    if not dates_found:
        print(f"Nenhuma particao encontrada em {_FACT_PREFIX}/")
        sys.exit(0)

    dates_sorted = sorted(dates_found)
    total = len(dates_sorted)
    print(f"\nParticoes encontradas: {total} datas ({dates_sorted[0]} a {dates_sorted[-1]})")
    print("Iniciando consolidacao...\n")

    pass_count = 0
    fail_count = 0
    total_rows = 0

    for i, date_str in enumerate(dates_sorted, 1):
        print(f"[{i:3d}/{total}] {date_str} ... ", end="", flush=True)
        try:
            s07_consolidate_pbi.run(pipeline_date=date_str)
            print("OK")
            pass_count += 1
        except Exception as exc:
            print(f"FALHOU — {exc}")
            fail_count += 1

    print(f"\n{'='*52}")
    print(f"Consolidacao concluida")
    print(f"  OK    : {pass_count}/{total}")
    print(f"  Falhou: {fail_count}/{total}")
    print(f"  Periodo: {dates_sorted[0]} a {dates_sorted[-1]}")
    if pass_count > 0:
        print(f"  Consulte manifest.csv para detalhes por data.")
    print(f"{'='*52}\n")

    sys.exit(0 if fail_count == 0 else 1)


if __name__ == "__main__":
    main()
