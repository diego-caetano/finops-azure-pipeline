#!/usr/bin/env python3
"""Diagnostico completo MARONI maio/2026 — 25 dias + verifica AKS."""

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

def read_blob_parquet(blob_name):
    data = cont.get_blob_client(blob_name).download_blob().readall()
    return pd.read_parquet(io.BytesIO(data))

def read_blob_csv(blob_name, sep=";"):
    data = cont.get_blob_client(blob_name).download_blob().readall()
    return pd.read_csv(io.BytesIO(data), sep=sep)

SEP = "=" * 60

# ── 1. TOTAIS COMPLETOS (25 dias) via parquet mensal ─────────
print(SEP)
print("1. MARONI MAIO COMPLETO (fact_allocated_cost_2026-05.parquet)")
print(SEP)

df_alloc = read_blob_parquet(
    "gold/fact_allocated_cost_consolidated/fact_allocated_cost_2026-05.parquet"
)
df_alloc["Date"] = df_alloc["Date"].astype(str)
maroni = df_alloc[
    (df_alloc["Cliente"] == "MARONI") &
    df_alloc["Date"].str.startswith("2026-05")
].copy()

print(f"Total linhas MARONI maio: {len(maroni):,}")
print(f"\nAllocatedCost por AllocationMethod (25 dias):")
for method, val in (maroni.groupby("AllocationMethod")["AllocatedCost"]
                    .sum().sort_values(ascending=False).items()):
    print(f"  {method:<30} R$ {val:>12,.2f}")

total_alloc = maroni["AllocatedCost"].sum()
total_cost  = maroni["Cost"].sum()
print(f"\n  {'TOTAL AllocatedCost':<30} R$ {total_alloc:>12,.2f}")
print(f"  {'TOTAL Cost (bruto)':<30} R$ {total_cost:>12,.2f}")
print(f"  Fan-out ratio Cost/Alloc:        {total_cost/total_alloc:.2f}x")

# ── 2. SQLPOOL pool dedicado — custo que deveria ir 100% MARONI
print()
print(SEP)
print("2. Pool sqldb-elastic-pool-vcore-prd-maroni-cw (25 dias)")
print(SEP)

pool_sk = "SQLPOOL:sqldb-elastic-pool-vcore-prd-maroni-cw"
pool_all = df_alloc[
    (df_alloc["ServiceKey"] == pool_sk) &
    df_alloc["Date"].str.startswith("2026-05")
]
total_pool = pool_all["AllocatedCost"].sum()
maroni_pool_atual = pool_all[pool_all["Cliente"] == "MARONI"]["AllocatedCost"].sum()

print(f"Custo TOTAL do pool em maio:              R$ {total_pool:>10,.2f}")
print(f"Custo alocado para MARONI (atual):        R$ {maroni_pool_atual:>10,.2f}")
print(f"Custo distribuido para OUTROS clientes:   R$ {total_pool - maroni_pool_atual:>10,.2f}")
print(f"\nClientes que receberam custo deste pool:")
for cl, val in (pool_all.groupby("Cliente")["AllocatedCost"].sum()
                .sort_values(ascending=False).items()):
    marker = " <<< MARONI (deveria ser 100%)" if cl == "MARONI" else ""
    print(f"  {cl:<25} R$ {val:>10,.2f}{marker}")

print(f"\nSe pool fosse 100% MARONI, MARONI ganharia: R$ {total_pool - maroni_pool_atual:>10,.2f} a mais")
print(f"Novo total MARONI (pos-fix s03c):          R$ {total_alloc + (total_pool - maroni_pool_atual):>10,.2f}")

# ── 3. AKS — MARONI tem namespace? ───────────────────────────
print()
print(SEP)
print("3. AKS — verificar se MARONI tem namespace proprio (amostra 3 dias)")
print(SEP)

blobs_aks = sorted([
    b.name for b in cont.list_blobs(name_starts_with="gold/aks_allocation_share_opencost/")
    if b.name.endswith(".csv") and "/dt=2026-05-" in b.name
    and "namespaces_sem_alias" not in b.name
])
print(f"Arquivos AKS share de maio: {len(blobs_aks)}")

frames_aks = []
for b in blobs_aks[:3]:
    try:
        df_tmp = read_blob_csv(b)
        frames_aks.append(df_tmp)
    except Exception as e:
        print(f"  Erro lendo {b}: {e}")

if frames_aks:
    df_aks = pd.concat(frames_aks, ignore_index=True)
    if "Cliente" in df_aks.columns:
        maroni_aks = df_aks[df_aks["Cliente"] == "MARONI"]
        if maroni_aks.empty:
            print("INFO: MARONI NAO aparece no output do s03b (sem namespace proprio)")
            print("      O AKS_OPENCOST de R$3.005 vem de outra fonte")
        else:
            print(f"MARONI aparece {len(maroni_aks)} vezes no s03b (tem namespace proprio):")
            if "Namespace" in maroni_aks.columns:
                print(maroni_aks[["Date","Namespace","Share","AllocationMethod"]].head(10))
            else:
                print(maroni_aks.head(10))
    else:
        print(f"Colunas: {df_aks.columns.tolist()}")
        print(df_aks.head(5))
else:
    print("Nenhum arquivo AKS lido")

# Verificar AKS no allocated — de onde vem?
maroni_aks_alloc = maroni[maroni["AllocationMethod"] == "AKS_OPENCOST"]
if not maroni_aks_alloc.empty:
    print(f"\nAKS_OPENCOST no fact_allocated_cost MARONI:")
    print(f"  ServiceKeys envolvidos:")
    for sk, val in (maroni_aks_alloc.groupby("ServiceKey")["AllocatedCost"]
                    .sum().sort_values(ascending=False).head(10).items()):
        print(f"    {sk:<60} R$ {val:>8,.2f}")

# ── 4. r2s — classificacao do pool dedicado (1 dia) ──────────
print()
print(SEP)
print("4. r2s - EffectiveFinopsCliente do pool maroni (1 dia: 2026-05-15)")
print(SEP)

try:
    df_r2s = read_blob_parquet(
        "silver/resource_to_service/dt=2026-05-15/resource_to_service.parquet"
    )
    pool_r2s = df_r2s[df_r2s["ServiceKey"] == pool_sk]
    if not pool_r2s.empty:
        print("r2s entry do pool:")
        cols = ["ResourceId","ServiceKey","ServiceType","Rule","EffectiveFinopsCliente","Confidence"]
        print(pool_r2s[cols].to_string(index=False))
    else:
        print("Pool nao encontrado no r2s de 2026-05-15")
        # Buscar por substring
        sub = df_r2s[df_r2s["ServiceKey"].str.contains("maroni", case=False, na=False)]
        if not sub.empty:
            print("ServiceKeys com 'maroni':")
            print(sub[["ResourceId","ServiceKey","ServiceType","Rule","EffectiveFinopsCliente"]].to_string(index=False))
except Exception as e:
    print(f"Erro: {e}")

# ── 5. RESUMO FINANCEIRO FINAL ────────────────────────────────
print()
print(SEP)
print("5. RESUMO — MARONI MAIO IMPACTO DO BUG")
print(SEP)

custo_azure_cm   = 62_634.80
dedicado_atual   = maroni[maroni["AllocationMethod"] == "DEDICATED_TAG"]["AllocatedCost"].sum()
sqlpool_atual    = maroni[maroni["AllocationMethod"] == "SQLPOOL_CPU"]["AllocatedCost"].sum()
aks_atual        = maroni[maroni["AllocationMethod"] == "AKS_OPENCOST"]["AllocatedCost"].sum()
wp_atual         = maroni[maroni["AllocationMethod"] == "WEIGHT_PROPORTIONAL"]["AllocatedCost"].sum()
pool_perdido     = total_pool - maroni_pool_atual

print(f"Azure Cost Management (tag=MARONI):      R$ {custo_azure_cm:>12,.2f}")
print()
print("Pipeline ATUAL (bugado):")
print(f"  DEDICATED_TAG:                         R$ {dedicado_atual:>12,.2f}")
print(f"  SQLPOOL_CPU (pool compartilhado bug):  R$ {sqlpool_atual:>12,.2f}")
print(f"  AKS_OPENCOST (verificar se valido):    R$ {aks_atual:>12,.2f}")
print(f"  WEIGHT_PROPORTIONAL:                   R$ {wp_atual:>12,.2f}")
print(f"  TOTAL ATUAL:                           R$ {total_alloc:>12,.2f}")
print()
print(f"Custo do pool que esta indo para OUTROS: R$ {pool_perdido:>12,.2f}")
print()
print("Pipeline POS-FIX s03c (estimativa):")
print(f"  DEDICATED_TAG:                         R$ {dedicado_atual:>12,.2f}")
print(f"  SQLPOOL_CPU (pool 100% MARONI):        R$ {total_pool:>12,.2f}")
print(f"  AKS_OPENCOST (manter/remover):         R$ {aks_atual:>12,.2f}")
print(f"  WEIGHT_PROPORTIONAL:                   R$ {wp_atual:>12,.2f}")
print(f"  TOTAL POS-FIX:                         R$ {total_alloc + pool_perdido:>12,.2f}")
