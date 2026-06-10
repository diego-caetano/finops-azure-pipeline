# FinOps Pipeline — Histórico de Builds, Bugs e Validações

Arquivo de referência para auditorias e post-mortems. Não é carregado automaticamente
em conversas com Claude — acesse explicitamente quando precisar de contexto histórico.

---

## Workload Identity Setup — novo cluster {#workload-identity-setup}

```bash
# 1. Criar identity
az identity create --name <IDENTITY-NAME> --resource-group <RG-DO-CLUSTER> --location eastus

# 2. Pegar Client ID e Object ID
CLIENT_ID=$(az identity show --name <IDENTITY-NAME> --resource-group <RG> --query clientId -o tsv)
OBJECT_ID=$(az identity show --name <IDENTITY-NAME> --resource-group <RG> --query principalId -o tsv)

# 3. Reader no storage do FOCUS (subscription diferente)
az role assignment create \
  --assignee $OBJECT_ID --role "Storage Blob Data Reader" \
  --scope "/subscriptions/0535039d-8b8e-4ac1-9fce-9f727e8ba2b1/resourceGroups/azurecostexportfocus10/providers/Microsoft.Storage/storageAccounts/azurecostexportfocus10"

# 4. Contributor no storage de saída
az role assignment create \
  --assignee $OBJECT_ID --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/52d4423b-7ed9-4673-b8e2-fa21cdb83176/resourceGroups/rg-psl-kmm-finops-eus-prd/providers/Microsoft.Storage/storageAccounts/stpslkmmfinopseusprd/blobServices/default/containers/finops-v2"

# 5. Federated Credential
OIDC_ISSUER=$(az aks show --resource-group <RG> --name <CLUSTER> --query "oidcIssuerProfile.issuerUrl" -o tsv)
az identity federated-credential create \
  --name finops-aks-federated --identity-name <IDENTITY-NAME> --resource-group <RG> \
  --issuer $OIDC_ISSUER \
  --subject "system:serviceaccount:finops:finops-workload-identity-sa" \
  --audiences "api://AzureADTokenExchange"
```

Identidades provisionadas:

| Cluster | Identity name | CLIENT_ID | Data |
|---|---|---|---|
| aks-prd-newkmm | id-mg-kmm-finops-aks-prd-newkmm-eus-prd | `40995235-e00b-417e-83a6-4ca383f9d169` | 2026-04-30 |
| aks-prd-br-newkmm | id-mg-kmm-finops-aks-prd-br-newkmm-eus-prd | `e67c4ddc-1e3f-451b-a9bd-b41296bf2834` | 2026-05-04 |
| aks-hml-newkmm | id-mg-kmm-finops-aks-newkmm-eus-hml | `5eed8b89-7707-44cf-85e3-a6f86866a25f` | 2026-04-30 |
| aks-mg-kmm-firewall-eus-prd | id-mg-kmm-finops-aks-firewall-eus-prd | `23fe2b59-90a1-450d-8531-de76eba5340a` | 2026-05-04 |
| aks-mg-kmm-firewall-eus-hml | id-mg-kmm-finops-aks-firewall-eus-hml | `bffedfaa-5c53-4409-988c-c6884579cebc` | 2026-05-04 |

---

## Histórico de retrocarga — aks-prd-newkmm (cluster central)

| Build | Data | Resultado | Consolidado | Observação |
|---|---|---|---|---|
| original | 2026-05-01 a 05-15 | 15/15 OK | — | Primeira retrocarga |
| retro corretiva | 2026-05-15 a 05-20 | 6/6 OK | — | FOCUS_LAG_DAYS=3 aplicado |
| cq11 | 2026-05-26 | 24/24 OK | — | SQLPOOL NaN + DEFAULT + COMPARTILHADO |
| cq12 | 2026-05-26 | 24/24 OK | 1.596.542 linhas | AKS fan-out inflation |
| cq13 | 2026-05-26 | 25/25 OK | 1.675.919 linhas \| R$351.553 | +maio/25 |
| cq14 | 2026-05-27 | 25/25 OK | **308.379 linhas** (−82%) \| R$351.553 | meter fan-out + DEFAULT histórico |
| cq18 validação | 2026-05-28 | 3/3 OK | — | s03d validado (01–03/05) |
| cq18 completa | 2026-05-28 | 25/25 OK | 707.805 linhas \| R$351.552,92 | s03d backup/snapshots |
| cq19 | 2026-06-09 | 31/31 OK | 905.849 linhas | pool MARONI DEDICATED_TAG |
| cq1c | 2026-06-09 | 31/31 OK | 905.865 linhas | DiskGBAvg + pesos 70/30 |
| cq1d | 2026-06-10 | build OK | — | ACR build apenas (sem retrocarga) |

---

## Validação pipeline cq1c (2026-06-09, maio completo)

| Métrica | Valor |
|---|---|
| TotalAllocated | **100,0%** (todas as 31 datas) |
| Custo médio diário | ~R$ 13.400–15.700/dia |
| Total maio (01–31) | ~R$ 458K \| média ~R$ 14.780/dia |
| AKS_OPENCOST | ~R$ 1.500/dia via namespace |
| SQLPOOL | 8 pools; 1 dedicado (MARONI); 7 compartilhados CPU+DiskGB |

| AllocationMethod | AllocatedCost (maio) | Obs |
|---|---|---|
| DEDICATED_TAG | R$ 61.712,98 | MARONI (pool dedicado) |
| SQLPOOL_CPU | — | pools compartilhados |
| WEIGHT_PROPORTIONAL | ~R$ 37.950 | sem classificação específica |
| AKS_OPENCOST | ~R$ 33.116 | namespace por cluster |
| BACKUP_VAULT_ITEMS | ~R$ 1.917 | 2 vaults |
| BACKUP_SNAPSHOT_DIRECT | R$ 0 | snapshots tagueados → s03a |

**Reconciliação MARONI:**
- Azure Cost Management (tag=MARONI, maio): R$ 62.634
- Pipeline DEDICATED_TAG: R$ 61.712,98 (Δ ~R$ 921 = RI amortização via EffectiveCost) ✅
- Pipeline TOTAL MARONI: R$ 68.725,75 (inclui WEIGHT_PROPORTIONAL + AKS_OPENCOST)

Consolidado: `fact_allocated_cost_2026-05.parquet` — 905.865 linhas, 31 datas.
**Power BI:** arquivo `fact_allocated_cost_all.parquet` está obsoleto — apontar para `fact_allocated_cost_2026-05.parquet` ou usar folder connector (ver `docs/POWER_BI.md`).

---

## FOCUS_LAG_DAYS = 3 — rationale e evidências

Charges de um dia continuam aparecendo no FOCUS por 2–3 dias após a data de uso.
Ao rodar D-1, custo capturado era ~35% do valor real. Após D-3: projeção mensal ~R$ 447k (esperado ~R$ 420k).

Retrocarga corretiva D-3 executada em 2026-05-22 (6/6 OK):

| Data | TotalFactCost antes (D-1) | TotalFactCost depois (D-3) |
|---|---|---|
| 2026-05-15 | 4.726 | 12.408 (+163%) |
| 2026-05-16 | 4.883 | 12.752 (+161%) |
| 2026-05-17 | 4.672 | 12.693 (+171%) |
| 2026-05-18 | 4.825 | 12.945 (+168%) |
| 2026-05-19 | 5.004 | 13.342 (+167%) |
| 2026-05-20 | 4.338 | 12.861 (+197%) |

---

## Bugs resolvidos

### BUG — AKS_OPENCOST = R$ 0 (resolvido cq12, 2026-05-25)

**Causa:** `s02_classify.py` — `rule4_wins` tinha `& ~cond3` que bloqueava OVERRIDE_RG_AUTO quando MC_* RGs tinham tag COMPARTILHADO no rg_tags.csv.
**Fix:** removido `& ~cond3`. OVERRIDE_RG_AUTO tem prioridade sobre TAG_RG_SHARED para RGs MC_*.
Nota: `Rule` no r2s ainda mostra `TAG_RG_SHARED` (np.select não alterado), mas `ServiceKey=AKS:{cluster}` está correto.
**Validação:** AKS_OPENCOST R$ 1.503,50 (era R$ 0), 12.575 linhas, 4 clusters.

---

### BUG — AKS fan-out inflation (resolvido cq12, 2026-05-26)

**Problema:** recursos AKS sem namespace próprio (VMSS, Disks, LB, PVCs) recebiam N linhas por cliente (N = namespaces ativos), inflando AllocatedCost ~5x.
**Causa raiz:** CSVs de s03b por namespace eram concatenados ao `allocation_share_final` com múltiplos pares `(ServiceKey, Cliente)` duplicados.
**Fix:**
- `s05`: colapsa df_aks por `(Date, ServiceKey, Cliente)` após leitura
- `s06`: guard colapsa pares `(ServiceKey, Cliente)` duplicados antes do join

---

### BUG — Meter-level fan-out s06 + DEFAULT histórico s05 (resolvido cq14, 2026-05-27)

**Problema 1 (meter fan-out):** `fact_cost` tem N linhas por ResourceId (1 por meter FOCUS). Join com C clientes = N×C linhas. Parquet inflado 208× para VMSS.
**Fix s06:** agrega `fact_cost` por `(Date, ResourceId, ServiceKey)` somando Cost ANTES do join com shares. Resultado: 1 linha por (ResourceId × cliente).

**Problema 2 (DEFAULT histórico):** CSVs de s03b anteriores ao cq11 tinham `Cliente=DEFAULT`. O retro_fix.py não re-executa s03b — lê CSVs históricos do blob. s05 consumia sem filtrar.
**Fix s05:** filtra `Cliente=DEFAULT` de df_aks após dedup, com warning log.

| Antes (cq13) | Depois (cq14) |
|---|---|
| 1.675.919 linhas | 308.379 linhas (−82%) |
| DEFAULT: 17.403 | DEFAULT: 0 |
| AllocatedCost R$351.553 | AllocatedCost R$351.553 (idêntico) |

---

### BUG — 3 bugs de alocação (resolvidos cq11, 2026-05-26)

**BUG 1 — SQLPOOL NaN (R$64k / 19%):** `s05._read_csv_partition` lia todos os CSVs do prefix, incluindo arquivos de auditoria sem `AllocationMethod`. Fix: `_read_csv_partition` aceita `include_prefix`/`exclude_patterns`; guard em s06 descarta residuais com log.

**BUG 2 — AKS DEFAULT (R$4.824 / 1,4%):** namespaces com alias `DEFAULT` passavam filtro `!= "COMPARTILHADO"`. Fix: s03b adiciona `"DEFAULT"` à lista de exclusão; vai para `unmapped_cost`.

**BUG 3 — SQL COMPARTILHADO residual (R$673 / 0,2%):** s03c adicionava databases sem tag como `Cliente=COMPARTILHADO` em `all_share`. Fix: separa `tagged_rows`/`compartilhado_rows`; shares calculados sobre `total_tagged_weighted`; COMPARTILHADO só vai para detalhe com `Share=0`.

---

### BUG — Pool MARONI alocado como SQLPOOL_CPU (resolvido cq19, 2026-06-09)

**Problema:** pool `sqldb-elastic-pool-vcore-prd-maroni-cw` tem tag `FINOPS-CLIENTE=MARONI`, mas s03c ignorava `EffectiveFinopsCliente` e processava via métricas. Custo saía como `SQLPOOL_CPU`, ocultando R$ 49K como dedicado no Power BI ("Custo Dedicado" = R$ 12K em vez de R$ 62K).
**Fix:** s03c verifica `EffectiveFinopsCliente` do pool no r2s antes de chamar ARM API. Se dedicado → `Share=1.0`, `DEDICATED_TAG`, `SQLPOOL_DEDICATED_POOL`.

---

### BUG — storage metric DiskPercentAvg enganoso (resolvido cq1c, 2026-06-09/10)

**Problema:** `DiskPercentAvg` (% do max configurado por DB) tornava comparação injusta entre databases com limites diferentes configurados. Peso era 3% (0.03) — muito baixo para refletir estrutura de custo Azure.
**Fix:** substituído por `DiskGBAvg` (bytes absolutos → GB). Pesos ajustados: CPU 0.97→0.70, Disk 0.03→0.30 (aproxima estrutura de custo vCore Azure: 70% compute / 30% storage).

---

## Histórico de builds ACR

| Build ID | Data | Descrição |
|---|---|---|
| cq11 | 2026-05-26 | SQLPOOL NaN + AKS DEFAULT + SQL COMPARTILHADO |
| cq12 | 2026-05-26 | AKS fan-out + MC_* OVERRIDE_RG_AUTO fix |
| cq14 | 2026-05-27 | Meter fan-out + DEFAULT histórico |
| cq18 | 2026-05-28 | s03d backup/snapshots |
| cq19 | 2026-06-09 | Pool dedicado MARONI → DEDICATED_TAG |
| cq1b | 2026-06-09 | (intermediário — sem retrocarga) |
| cq1c | 2026-06-09 | DiskPercentAvg → DiskGBAvg + pesos 70/30 |
| cq1d | 2026-06-10 | ACR build sem alteração de código |
