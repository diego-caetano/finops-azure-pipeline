"""
s03d_backup.py — Alocação Azure Backup e Snapshots.

PARTE 1 — Snapshots: mapeamento direto por ResourceName → clientes_alias.
  Extrai segmento de VM do ResourceName (vm-{cliente}-prd-...) e busca em
  clientes_alias.csv. Fallback: RG → rg_tags.csv.
  AllocationMethod = BACKUP_SNAPSHOT_DIRECT | Share = 1.0

PARTE 2 — Recovery Services Vault: rateio por número de protected items.
  Lista via ARM API; resolve cliente por friendlyName → alias ou RG → rg_tags.
  Items sem mapeamento → redistribuídos proporcionalmente (padrão s03b DEFAULT).
  AllocationMethod = BACKUP_VAULT_ITEMS | DriverType = BACKUP_PROTECTED_ITEMS

Outputs em gold/backup_allocation_share/dt={date}/:
  backup_share_{date}.csv          — shares para s05 (Date, ServiceKey, Cliente, Share, DriverType, AllocationMethod)
  vault_detail_{date}.csv          — auditoria de vaults (VaultName, Cliente, ProtectedItems, TotalItems)
  snapshots_sem_alias_{date}.csv   — snapshots sem alias mapeável
"""
from __future__ import annotations

import logging
import os
import re
import time
from typing import Optional

import pandas as pd
import requests
from azure.identity import DefaultAzureCredential

from finops.storage import pipeline_client
from finops.utils.dates import resolve_pipeline_date, to_partition

logger = logging.getLogger(__name__)

_REFERENCE_PREFIX = os.getenv("REFERENCE_PREFIX", "config/reference")
_R2S_PREFIX = os.getenv("RESOURCE_TO_SERVICE_PREFIX", "silver/resource_to_service")
_BACKUP_OUT_PREFIX = os.getenv("BACKUP_OUT_PREFIX", "gold/backup_allocation_share")

_ARM_API_VERSION = "2023-04-01"
_ARM_BASE = "https://management.azure.com"
_ARM_TIMEOUT = 60
_ARM_RETRIES = 3
_ARM_RETRY_DELAY = 5.0

_COMPARTILHADO = "COMPARTILHADO"
_EXCLUDED_CLIENTS = {_COMPARTILHADO, "DEFAULT", ""}

_OUTPUT_COLUMNS = ["Date", "ServiceKey", "Cliente", "Share", "DriverType", "AllocationMethod"]


# ---------------------------------------------------------------------------
# ARM helpers
# ---------------------------------------------------------------------------

def _get_token(credential: DefaultAzureCredential) -> str:
    return credential.get_token("https://management.azure.com/.default").token


def _arm_get(url: str, token: str, params: dict | None = None) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    last_exc: Exception | None = None
    for attempt in range(1, _ARM_RETRIES + 2):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=_ARM_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_exc = exc
            if attempt <= _ARM_RETRIES:
                wait = _ARM_RETRY_DELAY * (2 ** (attempt - 1))
                logger.warning("⚠ ARM tentativa %d falhou: %s — aguardando %.1fs", attempt, exc, wait)
                time.sleep(wait)
    raise last_exc  # type: ignore[misc]


def _arm_get_all_pages(base_url: str, token: str) -> list[dict]:
    """Fetch all pages from an ARM list endpoint (handles nextLink pagination)."""
    items: list[dict] = []
    next_url: str | None = base_url
    params: dict | None = {"api-version": _ARM_API_VERSION}
    while next_url:
        data = _arm_get(next_url, token, params=params)
        items.extend(data.get("value", []))
        next_url = data.get("nextLink")
        params = None  # nextLink already contains api-version
    return items


# ---------------------------------------------------------------------------
# Client resolution helpers
# ---------------------------------------------------------------------------

def _resolve_by_alias(name: str, alias_df: pd.DataFrame) -> Optional[str]:
    """Resolve client from name using clientes_alias.csv.
    Same strategy as s03b: exact match, then alias-as-substring of name.
    """
    name_lower = name.lower()
    exact = alias_df[alias_df["Alias"].str.lower() == name_lower]
    if not exact.empty:
        return str(exact.iloc[0]["Cliente"])
    for _, row in alias_df.iterrows():
        alias = str(row["Alias"]).lower()
        if alias and alias in name_lower:
            return str(row["Cliente"])
    return None


def _extract_vm_segment(resource_name: str) -> str | None:
    """Extract the client-identifiable segment from 'vm-{segment}-prd-...' names.

    'vm-maroni-prd-vm-maroni-prd_DataDisk_0-20260515' → 'maroni'
    'vm-alianca_truck-prd-...' → 'alianca_truck'
    Returns None for names that don't start with 'vm-'.
    """
    m = re.match(r"^vm-([a-zA-Z0-9_]+)", resource_name.lower())
    return m.group(1) if m else None


def _resolve_snapshot_client(
    resource_name: str,
    resource_group: str,
    alias_df: pd.DataFrame,
    rg_tags: dict[str, str],
) -> Optional[str]:
    """Resolve client for a snapshot via 3-step fallback:
    1. VM segment extracted from name → alias lookup
    2. Full name → alias lookup (substring scan)
    3. RG → rg_tags
    """
    # 1. VM segment
    segment = _extract_vm_segment(resource_name)
    if segment:
        client = _resolve_by_alias(segment, alias_df)
        if client and client.upper() not in _EXCLUDED_CLIENTS:
            return client

    # 2. Full name
    client = _resolve_by_alias(resource_name, alias_df)
    if client and client.upper() not in _EXCLUDED_CLIENTS:
        return client

    # 3. RG fallback
    rg_client = rg_tags.get(resource_group.upper())
    if rg_client and rg_client not in _EXCLUDED_CLIENTS:
        return rg_client

    return None


def _resolve_item_client(
    item: dict,
    alias_df: pd.DataFrame,
    rg_tags: dict[str, str],
) -> Optional[str]:
    """Resolve client for a vault protected item via friendlyName → alias, then sourceRG → rg_tags."""
    props = item.get("properties", {})
    friendly_name = str(props.get("friendlyName", "") or "")
    source_rid = str(props.get("sourceResourceId", "") or "")

    # 1. VM segment from friendly name
    if friendly_name:
        segment = _extract_vm_segment(friendly_name)
        if segment:
            client = _resolve_by_alias(segment, alias_df)
            if client and client.upper() not in _EXCLUDED_CLIENTS:
                return client
        client = _resolve_by_alias(friendly_name, alias_df)
        if client and client.upper() not in _EXCLUDED_CLIENTS:
            return client

    # 2. RG extracted from sourceResourceId
    if source_rid:
        m = re.match(r"/subscriptions/[^/]+/resourceGroups/([^/]+)", source_rid, re.IGNORECASE)
        if m:
            rg_client = rg_tags.get(m.group(1).upper())
            if rg_client and rg_client not in _EXCLUDED_CLIENTS:
                return rg_client

    return None


# ---------------------------------------------------------------------------
# Part 1 — Snapshots
# ---------------------------------------------------------------------------

def _process_snapshots(
    r2s: pd.DataFrame,
    date_str: str,
    alias_df: pd.DataFrame,
    rg_tags: dict[str, str],
) -> tuple[list[dict], list[dict]]:
    """Map untagged snapshots to clients via ResourceName.

    Only processes snapshots not already handled by s03a (Rule != TAG_RESOURCE/TAG_RG_INHERITED).
    Returns (share_rows, audit_rows).
    """
    snapshot_mask = r2s["ResourceType"].str.lower() == "microsoft.compute/snapshots"
    untagged_mask = ~r2s["Rule"].isin(["TAG_RESOURCE", "TAG_RG_INHERITED"])
    to_process = r2s[snapshot_mask & untagged_mask].copy()

    if to_process.empty:
        logger.info("ℹ️  Nenhum snapshot não-tagueado para processar")
        return [], []

    logger.info("🔍 %d snapshots não-tagueados para mapear", len(to_process))

    share_rows: list[dict] = []
    audit_rows: list[dict] = []

    for _, row in to_process.iterrows():
        resource_name = str(row.get("ResourceName", "") or "")
        resource_group = str(row.get("ResourceGroupName", "") or "")
        service_key = str(row.get("ServiceKey", "") or "")

        client = _resolve_snapshot_client(resource_name, resource_group, alias_df, rg_tags)

        if client:
            share_rows.append({
                "Date": date_str,
                "ServiceKey": service_key,
                "Cliente": client,
                "Share": 1.0,
                "DriverType": "SNAPSHOT_DIRECT",
                "AllocationMethod": "BACKUP_SNAPSHOT_DIRECT",
            })
        else:
            audit_rows.append({
                "ResourceName": resource_name,
                "ResourceGroupName": resource_group,
                "ServiceKey": service_key,
                "Motivo": "Alias não encontrado e RG sem tag FINOPS-CLIENTE",
            })

    logger.info(
        "📊 Snapshots: %d mapeados direto, %d sem mapeamento (WEIGHT_PROPORTIONAL)",
        len(share_rows), len(audit_rows),
    )
    return share_rows, audit_rows


# ---------------------------------------------------------------------------
# Part 2 — Recovery Services Vault
# ---------------------------------------------------------------------------

def _fetch_protected_items(sub_id: str, rg: str, vault_name: str, token: str) -> list[dict]:
    # api-version é adicionado por _arm_get_all_pages via params — não incluir na URL base
    url = (
        f"{_ARM_BASE}/subscriptions/{sub_id}/resourceGroups/{rg}"
        f"/providers/Microsoft.RecoveryServices/vaults/{vault_name}"
        f"/backupProtectedItems"
    )
    return _arm_get_all_pages(url, token)


def _process_vault(
    vault_row: dict,
    date_str: str,
    alias_df: pd.DataFrame,
    rg_tags: dict[str, str],
    credential: DefaultAzureCredential,
) -> tuple[list[dict], list[dict]]:
    """Process one vault; returns (share_rows, detail_rows).

    On ARM failure → returns ([], []) so vault stays in WEIGHT_PROPORTIONAL.
    COMPARTILHADO items are redistributed proportionally to mapped clients.
    """
    vault_name = str(vault_row["VaultName"])
    rg = str(vault_row["ResourceGroup"])
    sub_id = str(vault_row["SubscriptionId"])
    service_key = str(vault_row["ServiceKey"])

    logger.info("🔒 Vault: %s (RG=%s)", vault_name, rg)

    try:
        token = _get_token(credential)
        items = _fetch_protected_items(sub_id, rg, vault_name, token)
    except Exception as exc:
        logger.warning(
            "⚠ ARM API falhou para vault %s: %s → mantendo WEIGHT_PROPORTIONAL", vault_name, exc
        )
        return [], []

    if not items:
        logger.warning("⚠ Vault %s sem protected items → mantendo WEIGHT_PROPORTIONAL", vault_name)
        return [], []

    logger.info("📋 %d protected items encontrados em %s", len(items), vault_name)

    client_counts: dict[str, int] = {}
    compartilhado_count = 0

    for item in items:
        client = _resolve_item_client(item, alias_df, rg_tags)
        friendly = item.get("properties", {}).get("friendlyName", "?")
        if client:
            client_counts[client] = client_counts.get(client, 0) + 1
            logger.debug("  ✅ %s → %s", friendly, client)
        else:
            compartilhado_count += 1
            logger.debug("  ⚠ %s → COMPARTILHADO (sem mapeamento)", friendly)

    total_items = len(items)
    mapped_count = sum(client_counts.values())

    if not client_counts:
        logger.warning(
            "⚠ Nenhum protected item mapeável em %s → WEIGHT_PROPORTIONAL", vault_name
        )
        return [], []

    if compartilhado_count > 0:
        logger.info(
            "  Redistribuindo %d item(s) COMPARTILHADO proporcionalmente para %d cliente(s)",
            compartilhado_count, len(client_counts),
        )

    share_rows: list[dict] = []
    detail_rows: list[dict] = []

    for client, count in client_counts.items():
        # share denominator = mapped_count → COMPARTILHADO absorbed proportionally
        share = round(count / mapped_count, 6)
        share_rows.append({
            "Date": date_str,
            "ServiceKey": service_key,
            "Cliente": client,
            "Share": share,
            "DriverType": "BACKUP_PROTECTED_ITEMS",
            "AllocationMethod": "BACKUP_VAULT_ITEMS",
        })
        detail_rows.append({
            "Date": date_str,
            "VaultName": vault_name,
            "Cliente": client,
            "ProtectedItems": count,
            "TotalItems": total_items,
            "CompartilhadoItems": compartilhado_count,
            "Share": share,
        })

    logger.info(
        "✅ Vault %s: %d clientes | %d mapped | %d COMPARTILHADO | soma_share=%.6f",
        vault_name, len(share_rows), mapped_count, compartilhado_count,
        sum(r["Share"] for r in share_rows),
    )
    return share_rows, detail_rows


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(pipeline_date: Optional[str] = None) -> None:
    date_str = resolve_pipeline_date(pipeline_date)
    logger.info("📅 s03d_backup  date=%s", date_str)

    store = pipeline_client()
    partition = to_partition(date_str)

    # ── Reference data ─────────────────────────────────────────────────────
    alias_df = store.read_csv(f"{_REFERENCE_PREFIX}/clientes_alias.csv")

    rg_tags: dict[str, str] = {}
    try:
        rg_df = store.read_csv(f"{_REFERENCE_PREFIX}/rg_tags.csv")
        for _, row in rg_df.iterrows():
            rg_tags[str(row["ResourceGroupName"]).strip().upper()] = (
                str(row["FinopsCliente"]).strip().upper()
            )
        logger.info("📋 rg_tags: %d entradas", len(rg_tags))
    except Exception as exc:
        logger.warning("⚠ rg_tags.csv indisponível: %s", exc)

    try:
        vaults_df = store.read_csv(f"{_REFERENCE_PREFIX}/backup_vaults.csv")
    except Exception as exc:
        logger.warning("⚠ backup_vaults.csv indisponível: %s — pulando vaults", exc)
        vaults_df = pd.DataFrame()

    # ── Resource-to-service ────────────────────────────────────────────────
    r2s = store.read_parquet(f"{_R2S_PREFIX}/{partition}/resource_to_service.parquet")
    logger.info("📊 r2s: %d linhas", len(r2s))

    all_shares: list[dict] = []
    all_snap_audit: list[dict] = []
    all_vault_detail: list[dict] = []

    # ── Part 1: Snapshots ──────────────────────────────────────────────────
    logger.info("\n== PARTE 1: SNAPSHOTS ==")
    snap_shares, snap_audit = _process_snapshots(r2s, date_str, alias_df, rg_tags)
    all_shares.extend(snap_shares)
    all_snap_audit.extend(snap_audit)

    # ── Part 2: Vaults ────────────────────────────────────────────────────
    logger.info("\n== PARTE 2: VAULTS ==")
    if not vaults_df.empty:
        credential = DefaultAzureCredential()
        for _, vault_row in vaults_df.iterrows():
            v_shares, v_detail = _process_vault(
                vault_row.to_dict(), date_str, alias_df, rg_tags, credential
            )
            all_shares.extend(v_shares)
            all_vault_detail.extend(v_detail)
    else:
        logger.info("ℹ️  Nenhum vault configurado em backup_vaults.csv")

    # ── Output ────────────────────────────────────────────────────────────
    if not all_shares:
        logger.info("ℹ️  Nenhum share de backup gerado para %s — nada a gravar", date_str)
        return

    base = f"{_BACKUP_OUT_PREFIX}/{partition}"

    df_shares = pd.DataFrame(all_shares, columns=_OUTPUT_COLUMNS)
    share_blob = f"{base}/backup_share_{date_str}.csv"
    store.write_csv(share_blob, df_shares)
    logger.info("⬆️  Gravado: %s (%d linhas)", share_blob, len(df_shares))

    if all_vault_detail:
        df_vault_detail = pd.DataFrame(all_vault_detail)
        vault_blob = f"{base}/vault_detail_{date_str}.csv"
        store.write_csv(vault_blob, df_vault_detail)
        logger.info("📋 Detalhe vaults: %d → %s", len(df_vault_detail), vault_blob)

    if all_snap_audit:
        df_snap_audit = pd.DataFrame(all_snap_audit)
        audit_blob = f"{base}/snapshots_sem_alias_{date_str}.csv"
        store.write_csv(audit_blob, df_snap_audit)
        logger.info("📋 Snapshots sem alias: %d → %s", len(df_snap_audit), audit_blob)

    n_snap = sum(1 for r in all_shares if r["AllocationMethod"] == "BACKUP_SNAPSHOT_DIRECT")
    n_vault = sum(1 for r in all_shares if r["AllocationMethod"] == "BACKUP_VAULT_ITEMS")
    logger.info(
        "✅ s03d_backup concluído: %d shares totais (%d snapshot_direct, %d vault_items)",
        len(all_shares), n_snap, n_vault,
    )
