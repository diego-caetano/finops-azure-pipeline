#!/usr/bin/env python3
"""Diagnostico de discrepancia MARONI maio/2026."""

import io
import os
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

def _load_dotenv():
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

import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

ACCOUNT   = os.environ.get("PIPELINE_STORAGE_ACCOUNT", "stpslkmmfinopseusprd")
CONTAINER = os.environ.get("PIPELINE_CONTAINER", "finops-v2")

cred   = DefaultAzureCredential()
client = BlobServiceClient(f"https://{ACCOUNT}.blob.core.windows.net", credential=cred)
cont   = client.get_container_client(CONTAINER)

def read_blob_parquet(blob_name: str) -> pd.DataFrame:
    data = cont.get_blob_client(blob_name).download_blob().readall()
    return pd.read_parquet(io.BytesIO(data))

def read_blob_csv(blob_name: str, sep=";") -> pd.DataFrame:
    data = cont.get_blob_client(blob_name).download_blob().readall()
    return pd.read_csv(io.BytesIO(data), sep=sep)

SEP = "=" * 60

# ── 1. AllocatedCost MARONI maio por AllocationMethod ────────
print(SEP)
print("1. fact_allocated_cost_2026-05.parquet - MARONI completo")
print(SEP)

df_alloc = read_blob_parquet(
    "gold/fact_allocated_cost_consolidated/fact_allocated_cost_2026-05.parquet"
)
df_alloc["Date"] = df_alloc["Date"].astype(str)
maroni_maio = df_alloc[
    (df_alloc["Cliente"] == "MARONI") &
    df_alloc["Date"].str.startswith("2026-05")
].copy()

print(f"Total linhas MARONI maio: {len(maroni_maio):,}")
print()
print("AllocatedCost por AllocationMethod:")
for method, val in (maroni_maio.groupby("AllocationMethod")["AllocatedCost"]
                    .sum().sort_values(ascending=False).items()):
    print(f"  {method:<30} R$ {val:>12,.2f}")
print(f"\n  {'TOTAL MARONI MAIO':<30} R$ {maroni_maio['AllocatedCost'].sum():>12,.2f}")

# ── 2. Recursos MARONI via SQLPOOL_CPU — o que compoe os R$50k ─
print()
print(SEP)
print("2. SQLPOOL_CPU MARONI - ServiceKeys e custo por pool")
print(SEP)

maroni_sql = maroni_maio[maroni_maio["AllocationMethod"] == "SQLPOOL_CPU"]
print(f"Linhas SQLPOOL_CPU: {len(maroni_sql):,}")
print()
print("Custo por ServiceKey (pool):")
for sk, val in (maroni_sql.groupby("ServiceKey")["AllocatedCost"]
                .sum().sort_values(ascending=False).head(20).items()):
    print(f"  {sk:<40} R$ {val:>10,.2f}")

print()
print("Custo por data (amostras):")
daily = maroni_sql.groupby("Date")["AllocatedCost"].sum().sort_values(ascending=False)
print(f"  Max dia: R$ {daily.max():,.2f}  |  Min dia: R$ {daily.min():,.2f}  |  Media: R$ {daily.mean():,.2f}")

# ── 3. DEDICATED_TAG MARONI — ResourceGroups e recursos ───────
print()
print(SEP)
print("3. DEDICATED_TAG MARONI - top ResourceGroups")
print(SEP)

maroni_ded = maroni_maio[maroni_maio["AllocationMethod"] == "DEDICATED_TAG"]
print(f"Linhas DEDICATED_TAG: {len(maroni_ded):,}")
print()
print("Custo por ResourceGroupName:")
for rg, val in (maroni_ded.groupby("ResourceGroupName")["AllocatedCost"]
                .sum().sort_values(ascending=False).head(15).items()):
    print(f"  {rg:<50} R$ {val:>10,.2f}")

# ── 4. resource_to_service amostra — classificacao MARONI ─────
print()
print(SEP)
print("4. resource_to_service - Rules aplicadas para MARONI (amostra 5 dias)")
print(SEP)

blobs_r2s = sorted([
    b.name for b in cont.list_blobs(name_starts_with="silver/resource_to_service/")
    if b.name.endswith(".parquet") and "/dt=2026-05-" in b.name
])
print(f"Arquivos r2s de maio: {len(blobs_r2s)}")

frames_r2s = [read_blob_parquet(b) for b in blobs_r2s[:5]]
df_r2s = pd.concat(frames_r2s, ignore_index=True)

maroni_r2s = df_r2s[df_r2s["EffectiveFinopsCliente"] == "MARONI"]
print(f"\nRecursos MARONI na amostra ({len(maroni_r2s)} linhas, {maroni_r2s['ResourceId'].nunique()} ResourceIds unicos):")
print()
print("Rule x ServiceType:")
tbl = (maroni_r2s.groupby(["Rule", "ServiceType"])["ResourceId"]
       .nunique().reset_index().rename(columns={"ResourceId": "RecursoUnicos"}))
print(tbl.to_string(index=False))

# ── 5. SQL Pool — quais pools tem recursos MARONI ─────────────
print()
print(SEP)
print("5. SQL Pools com recursos MARONI (r2s amostra 5 dias)")
print(SEP)

maroni_pools = maroni_r2s[maroni_r2s["ServiceType"] == "sql_pool"]
if not maroni_pools.empty:
    print("ServiceKeys dos SQL pools com MARONI:")
    for sk in maroni_pools["ServiceKey"].unique():
        print(f"  {sk}")
    print()
    print("ResourceGroupName dos pools MARONI:")
    for rg in maroni_pools["ResourceGroupName"].unique():
        print(f"  {rg}")
else:
    print("Nenhum sql_pool direto classificado para MARONI")

# SQLPOOL via ServiceKey no allocated
sql_sks = maroni_sql["ServiceKey"].unique()
print(f"\nServiceKeys que entregam SQLPOOL_CPU para MARONI:")
for sk in sorted(sql_sks):
    print(f"  {sk}")

# ── 6. Contexto — comparar com outros clientes no mesmo pool ──
print()
print(SEP)
print("6. Comparacao - todos os clientes nos mesmos SQL Pools")
print(SEP)

if len(sql_sks) > 0:
    same_pool = df_alloc[
        df_alloc["ServiceKey"].isin(sql_sks) &
        (df_alloc["AllocationMethod"] == "SQLPOOL_CPU") &
        df_alloc["Date"].str.startswith("2026-05")
    ]
    print("AllocatedCost por Cliente nos pools de MARONI:")
    for cl, val in (same_pool.groupby("Cliente")["AllocatedCost"]
                    .sum().sort_values(ascending=False).items()):
        marker = " <<< MARONI" if cl == "MARONI" else ""
        print(f"  {cl:<25} R$ {val:>10,.2f}{marker}")
    print(f"\nTotal pool: R$ {same_pool['AllocatedCost'].sum():,.2f}")

print()
print(SEP)
print("CONCLUSAO")
print(SEP)
total = maroni_maio["AllocatedCost"].sum()
ded   = maroni_maio[maroni_maio["AllocationMethod"]=="DEDICATED_TAG"]["AllocatedCost"].sum()
sql   = maroni_maio[maroni_maio["AllocationMethod"]=="SQLPOOL_CPU"]["AllocatedCost"].sum()
wp    = maroni_maio[maroni_maio["AllocationMethod"]=="WEIGHT_PROPORTIONAL"]["AllocatedCost"].sum()
aks   = maroni_maio[maroni_maio["AllocationMethod"]=="AKS_OPENCOST"]["AllocatedCost"].sum()

print(f"Azure Cost Management (tag=MARONI, BilledCost): R$ 62.634")
print(f"Pipeline - DEDICATED_TAG:                        R$ {ded:>10,.2f}  (recursos com tag direta)")
print(f"Pipeline - SQLPOOL_CPU:                          R$ {sql:>10,.2f}  (share CPU em pools compartilhados)")
print(f"Pipeline - WEIGHT_PROPORTIONAL:                  R$ {wp:>10,.2f}  (rateio por peso)")
print(f"Pipeline - AKS_OPENCOST:                         R$ {aks:>10,.2f}  (namespace AKS)")
print(f"Pipeline - TOTAL MARONI:                         R$ {total:>10,.2f}")
print()
print("Hipotese: os R$50K de SQLPOOL_CPU correspondem a databases/pools")
print("tagueados como MARONI no Azure, que aparecem nos R$62K do portal")
print("mas sao processados via s03c (CPU share) em vez de DEDICATED_TAG.")
