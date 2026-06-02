#!/usr/bin/env python3
"""Diagnóstico AKS fan-out e DEFAULT — lê dados do blob e imprime métricas."""

import io
import os
import sys
from pathlib import Path

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
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

_load_dotenv()

import pandas as pd
from finops.storage import pipeline_client
from finops.utils.dates import to_partition

DATE = os.getenv("DIAG_DATE", "2026-05-01")
store = pipeline_client()
partition = to_partition(DATE)

print(f"\n{'='*60}")
print(f"DIAGNÓSTICO AKS FANOUT — {DATE}")
print(f"{'='*60}")

# ── 1. s03b CSVs brutos ────────────────────────────────────────
print("\n[1] s03b CSVs brutos (gold/aks_allocation_share_opencost)")
aks_prefix = f"gold/aks_allocation_share_opencost/{partition}/"
blobs = [b for b in store.list_blobs(aks_prefix)
         if b.endswith(".csv") and "namespaces_sem_alias" not in b]
print(f"  Arquivos: {[b.split('/')[-1] for b in blobs]}")

frames = [store.read_csv(b) for b in blobs]
if frames:
    df_aks_raw = pd.concat(frames, ignore_index=True)
    print(f"  Total linhas: {len(df_aks_raw)}")
    print(f"  Colunas: {list(df_aks_raw.columns)}")
    print(f"  Unique (ServiceKey, Cliente): {df_aks_raw.groupby(['ServiceKey','Cliente']).ngroups if 'ServiceKey' in df_aks_raw.columns else 'N/A'}")
    if "Cliente" in df_aks_raw.columns:
        n_default = (df_aks_raw["Cliente"].astype(str).str.upper() == "DEFAULT").sum()
        print(f"  Linhas DEFAULT: {n_default}")
    if "ServiceKey" in df_aks_raw.columns and "Share" in df_aks_raw.columns:
        for sk in df_aks_raw["ServiceKey"].unique():
            sub = df_aks_raw[df_aks_raw["ServiceKey"] == sk]
            print(f"  {sk}: {len(sub)} linhas, share_sum={sub['Share'].sum():.4f}, "
                  f"unique_clientes={sub['Cliente'].nunique() if 'Cliente' in sub.columns else '?'}")
else:
    print("  Nenhum CSV encontrado")

# ── 2. allocation_share_final ──────────────────────────────────
print("\n[2] allocation_share_final")
try:
    df_shares = store.read_parquet(f"gold/allocation_share_final/{partition}/allocation_share_final.parquet")
    print(f"  Total linhas: {len(df_shares)}")
    aks_shares = df_shares[df_shares["ServiceKey"].str.startswith("AKS:", na=False)]
    print(f"  Linhas AKS: {len(aks_shares)}")
    if "Cliente" in aks_shares.columns:
        n_default = (aks_shares["Cliente"].astype(str).str.upper() == "DEFAULT").sum()
        print(f"  Linhas DEFAULT (AKS): {n_default}")
    for sk in aks_shares["ServiceKey"].unique():
        sub = aks_shares[aks_shares["ServiceKey"] == sk]
        print(f"  {sk}: {len(sub)} clientes, share_sum={sub['Share'].sum():.6f}")
except Exception as e:
    print(f"  ERRO: {e}")

# ── 3. fact_cost — contagem por ResourceId (VMSS) ─────────────
print("\n[3] fact_cost — linhas por ResourceId (top-5 mais repetidos)")
try:
    df_fc = store.read_parquet(f"silver/fact_cost/{partition}/fact_cost.parquet")
    print(f"  Total linhas: {len(df_fc)}")
    counts = df_fc.groupby("ResourceId").size().sort_values(ascending=False)
    for rid, cnt in counts.head(5).items():
        cost_sum = df_fc[df_fc["ResourceId"] == rid]["Cost"].sum()
        rname = df_fc[df_fc["ResourceId"] == rid]["ResourceName"].iloc[0] if len(df_fc[df_fc["ResourceId"] == rid]) > 0 else ""
        print(f"  {cnt:3d}x  {rname or rid[-60:]}  total_cost={cost_sum:.2f}")
except Exception as e:
    print(f"  ERRO: {e}")

# ── 4. fact_allocated_cost — DEFAULT e fan-out ────────────────
print("\n[4] fact_allocated_cost — DEFAULT e fan-out AKS")
try:
    df_fac = store.read_parquet(f"gold/fact_allocated_cost/{partition}/fact_allocated_cost_{DATE}.parquet")
    print(f"  Total linhas: {len(df_fac)}")
    n_default = (df_fac["Cliente"].astype(str).str.upper() == "DEFAULT").sum()
    print(f"  Linhas DEFAULT: {n_default}")
    if n_default > 0:
        default_cost = df_fac[df_fac["Cliente"].str.upper() == "DEFAULT"]["AllocatedCost"].sum()
        print(f"  AllocatedCost DEFAULT: R$ {default_cost:.2f}")

    aks_fac = df_fac[df_fac["AllocationMethod"] == "AKS_OPENCOST"] if "AllocationMethod" in df_fac.columns else df_fac[df_fac.get("ServiceKey", pd.Series()).str.startswith("AKS:", na=False)]
    print(f"  Linhas AKS_OPENCOST: {len(aks_fac)}")

    if not aks_fac.empty and "ResourceId" in aks_fac.columns:
        # Top-3 ResourceIds com mais linhas por cliente
        per_rc = aks_fac.groupby(["ResourceId","Cliente"]).size().reset_index(name="n")
        top = per_rc.groupby("ResourceId")["n"].max().sort_values(ascending=False).head(3)
        print("  Top-3 ResourceIds (max linhas/cliente):")
        for rid, n in top.items():
            rname = aks_fac[aks_fac["ResourceId"]==rid]["ResourceName"].iloc[0] if "ResourceName" in aks_fac.columns else rid[-40:]
            cost_real = aks_fac[aks_fac["ResourceId"]==rid]["Cost"].sum()
            alloc = aks_fac[aks_fac["ResourceId"]==rid]["AllocatedCost"].sum()
            share_sum = aks_fac[aks_fac["ResourceId"]==rid]["Share"].sum()
            print(f"    {n:3d} linhas/cli  {rname}  cost_sum={cost_real:.2f}  alloc_sum={alloc:.2f}  share_sum={share_sum:.4f}")

        # Ratio total AKS
        cost_real_total = aks_fac.drop_duplicates(["ResourceId"])["Cost"].sum()
        alloc_total = aks_fac["AllocatedCost"].sum()
        print(f"  AKS cost_real (dedup ResourceId): {cost_real_total:.2f}")
        print(f"  AKS AllocatedCost total: {alloc_total:.2f}")
        print(f"  Ratio: {alloc_total/cost_real_total:.2f}x" if cost_real_total > 0 else "  Ratio: N/A")
except Exception as e:
    print(f"  ERRO: {e}")

print(f"\n{'='*60}")
print("FIM DO DIAGNÓSTICO")
print(f"{'='*60}\n")
