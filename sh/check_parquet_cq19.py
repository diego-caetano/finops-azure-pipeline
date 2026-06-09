#!/usr/bin/env python3
"""Verifica AllocationMethod no parquet cq19 para MARONI."""
import io, os, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

def _load_dotenv():
    from pathlib import Path
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if not env_file.exists():
        return
    with open(env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())

_load_dotenv()

import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

ACCOUNT   = os.environ.get("PIPELINE_STORAGE_ACCOUNT", "stpslkmmfinopseusprd")
CONTAINER = os.environ.get("PIPELINE_CONTAINER", "finops-v2")
cred   = DefaultAzureCredential()
client = BlobServiceClient(f"https://{ACCOUNT}.blob.core.windows.net", credential=cred)
cont   = client.get_container_client(CONTAINER)

def read_parquet(blob):
    data = cont.get_blob_client(blob).download_blob().readall()
    return pd.read_parquet(io.BytesIO(data))

print("Lendo fact_allocated_cost_2026-05.parquet...")
df = read_parquet("gold/fact_allocated_cost_consolidated/fact_allocated_cost_2026-05.parquet")
df["Date"] = df["Date"].astype(str)

maroni = df[(df["Cliente"] == "MARONI") & df["Date"].str.startswith("2026-05")]

print(f"\nMARONI maio — {len(maroni):,} linhas")
print("\nAllocatedCost por AllocationMethod:")
for m, v in maroni.groupby("AllocationMethod")["AllocatedCost"].sum().sort_values(ascending=False).items():
    print(f"  {m:<30} R$ {v:>12,.2f}")
print(f"\n  TOTAL                          R$ {maroni['AllocatedCost'].sum():>12,.2f}")

print("\n--- Pool MARONI no parquet ---")
pool_sk = "SQLPOOL:sqldb-elastic-pool-vcore-prd-maroni-cw"
pool_rows = df[df["ServiceKey"] == pool_sk]
if not pool_rows.empty:
    print(f"ServiceKey encontrado: {pool_sk}")
    print(f"AllocationMethods presentes: {pool_rows['AllocationMethod'].unique().tolist()}")
    print(f"Clientes: {pool_rows['Cliente'].unique().tolist()[:5]}")
    print(f"AllocatedCost total pool: R$ {pool_rows['AllocatedCost'].sum():,.2f}")
    print(f"AllocatedCost MARONI no pool: R$ {pool_rows[pool_rows['Cliente']=='MARONI']['AllocatedCost'].sum():,.2f}")
else:
    print("ServiceKey NAO encontrado no parquet")
