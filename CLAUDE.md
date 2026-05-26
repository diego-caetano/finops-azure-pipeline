# FinOps Allocation Pipeline — Contexto do Projeto

## O que é este projeto

Pipeline de alocação de custos Azure migrado de PowerShell + Azure Automation
para Python + Kubernetes CronJob. Consome FOCUS Report (parquet) como fonte.

O objetivo central é distribuir o custo Azure entre clientes de forma auditável:
- Recursos com tag `FINOPS-CLIENTE=ClienteX` → 100% para esse cliente (dedicado)
- RG com tag `FINOPS-CLIENTE=ClienteX` → recursos sem tag herdam do RG
- RG com tag `FINOPS-CLIENTE=COMPARTILHADO` → entra no rateio proporcional
- Clusters AKS compartilhados → rateio pelo consumo real via OpenCost por namespace
- Recursos sem tag e sem tag no RG → rateio pelo peso financeiro de cada cliente

---

## Escopo de subscriptions

O pipeline processa **apenas** recursos das subscriptions KMM. O FOCUS Report cobre todo o enrollment Azure EA — o filtro é aplicado em `s01_ingest_focus.py` via `BILLING_SUBSCRIPTION_IDS`.

| Subscription | Nome | ID |
|---|---|---|
| KMM PRD | Azure EA - KMM - PRD | `52d4423b-7ed9-4673-b8e2-fa21cdb83176` |
| KMM DEV-TEST | Azure EA - KMM - DEV-TEST | `3f6d197f-f70b-4c2c-b981-8bb575d47a7a` |

**Atenção:** o FOCUS armazena `SubscriptionId` (via `SubAccountId`) no formato ARM completo `/subscriptions/{guid}`. O filtro em s01 normaliza esse valor antes de comparar com os GUIDs da constante.

---

## Três Storage Accounts — papéis distintos

### 1. Storage de ENTRADA — FOCUS Report (somente leitura)
```
Account     : azurecostexportfocus10
Subscription: 0535039d-8b8e-4ac1-9fce-9f727e8ba2b1  (Azure EA - NSTECH - PRD)
Container   : azurecostexportfocus10
Prefixo     : costexport/azure-cost-export-focus10-52548376/
Estrutura   : costexport/azure-cost-export-focus10-52548376/<yyyyMMdd-yyyyMMdd>/<arquivo>
Exemplo     : costexport/azure-cost-export-focus10-52548376/20260501-20260531/data.parquet
```
O pipeline **nunca escreve** neste storage.
Permissão necessária: `Storage Blob Data Reader`

Lógica de seleção do arquivo (equivalente ao script 02 PS):
1. Lista partições `yyyyMMdd-yyyyMMdd` sob o prefixo
2. Ordena pelas mais recentes (até 6 períodos para trás)
3. Seleciona o arquivo cujo conteúdo contenha a `UsageDate` alvo
4. FOCUS usa `ChargePeriodStart` como coluna de data

### 2. Storage de SAÍDA — Pipeline Python (leitura e escrita)
```
Account     : stpslkmmfinopseusprd
Subscription: 52d4423b-7ed9-4673-b8e2-fa21cdb83176
RG          : rg-psl-kmm-finops-eus-prd
Container   : finops-v2
```
Permissão necessária: `Storage Blob Data Contributor`

Estrutura de pastas no container `finops-v2`:
```
finops-v2/
  config/reference/
    cluster_config.csv           ← clusters AKS (tipo, URL, cliente dedicado)
    clientes_alias.csv           ← alias namespace → cliente (89 entradas)
    aks_system_namespaces.csv    ← namespaces de sistema a redistribuir
    overrides_rg_auto.csv        ← padrões MC_ gerados automaticamente
    rg_tags.csv                  ← tags FINOPS-CLIENTE dos RGs (gerado diário)

  silver/fact_cost/dt=yyyy-MM-dd/
  silver/resource_to_service/dt=yyyy-MM-dd/
  silver/client_weights/dt=yyyy-MM-dd/

  gold/aks_allocation_share_opencost/dt=yyyy-MM-dd/
  gold/sql_pool_allocation_share/dt=yyyy-MM-dd/
    sqlpool_detail_all_<date>.csv
    sqlpool_allocation_share_all_<date>.csv
    databases_sem_tag_<date>.csv      ← databases sem tag FINOPS-CLIENTE (alocados como COMPARTILHADO)
  gold/allocation_share_final/dt=yyyy-MM-dd/
  gold/fact_allocated_cost/dt=yyyy-MM-dd/
  gold/fact_allocated_cost_consolidated/
    fact_allocated_cost_all.parquet  ← histórico completo para Power BI
    manifest.csv                     ← uma linha por data (Status, Rows, TotalAllocated)
```

### 3. Storage do Pipeline PS atual (não tocar)
```
Account  : stpslkmmfinopseusprd
Container: finops   ← manter intacto, pipeline PS continua rodando
```

---

## Autenticação — Workload Identity

O cluster `aks-hml-newkmm` (e os demais) já têm OIDC + Workload Identity habilitados.

Setup necessário — fazer uma vez por cluster compartilhado:

```
1. Criar User-Assigned Managed Identity
2. Dar Storage Blob Data Reader  em azurecostexportfocus10  (sub 0535039d)
3. Dar Storage Blob Data Contributor em stpslkmmfinopseusprd/finops-v2 (sub 52d4423b)
4. Criar Federated Credential vinculando ao ServiceAccount finops/finops-workload-identity-sa
5. Preencher <CLIENT_ID> no k8s/cronjob-aks.yaml
```

Identidades configuradas:

| Cluster | Identity name | Resource Group | Status |
|---|---|---|---|
| aks-prd-newkmm | id-mg-kmm-finops-aks-prd-newkmm-eus-prd | RG-US-PRD-KMM | ✅ operacional |
| aks-prd-br-newkmm | id-mg-kmm-finops-aks-prd-br-newkmm-eus-prd | RG-PRD-KMM | ✅ operacional |
| aks-hml-newkmm | id-mg-kmm-finops-aks-newkmm-eus-hml | RG-US-PRD-KMM | ✅ operacional |
| aks-mg-kmm-firewall-eus-prd | id-mg-kmm-finops-aks-firewall-eus-prd | RG-US-PRD-KMM | ✅ operacional |
| aks-mg-kmm-firewall-eus-hml | id-mg-kmm-finops-aks-firewall-eus-hml | RG-DEV-KMM (sub: 3f6d197f) | ✅ operacional |

Comandos para criar e configurar cada identity:
```bash
# 1. Criar identity (repetir para cada cluster)
az identity create \
  --name <IDENTITY-NAME> \
  --resource-group <RG-DO-CLUSTER> \
  --location eastus

# 2. Pegar Client ID e Object ID
CLIENT_ID=$(az identity show --name <IDENTITY-NAME> --resource-group <RG> --query clientId -o tsv)
OBJECT_ID=$(az identity show --name <IDENTITY-NAME> --resource-group <RG> --query principalId -o tsv)

# 3. Reader no storage do FOCUS (subscription diferente)
az role assignment create \
  --assignee $OBJECT_ID \
  --role "Storage Blob Data Reader" \
  --scope "/subscriptions/0535039d-8b8e-4ac1-9fce-9f727e8ba2b1/resourceGroups/azurecostexportfocus10/providers/Microsoft.Storage/storageAccounts/azurecostexportfocus10"

# 4. Contributor no storage de saída
az role assignment create \
  --assignee $OBJECT_ID \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/52d4423b-7ed9-4673-b8e2-fa21cdb83176/resourceGroups/rg-psl-kmm-finops-eus-prd/providers/Microsoft.Storage/storageAccounts/stpslkmmfinopseusprd/blobServices/default/containers/finops-v2"

# 5. Federated Credential
OIDC_ISSUER=$(az aks show --resource-group <RG> --name <CLUSTER> --query "oidcIssuerProfile.issuerUrl" -o tsv)
az identity federated-credential create \
  --name finops-aks-federated \
  --identity-name <IDENTITY-NAME> \
  --resource-group <RG> \
  --issuer $OIDC_ISSUER \
  --subject "system:serviceaccount:finops:finops-workload-identity-sa" \
  --audiences "api://AzureADTokenExchange"
```

---

## Hierarquia de classificação de recursos (s02_classify.py)

```
1. Tag FINOPS-CLIENTE no recurso (≠ COMPARTILHADO, ≠ vazio)
   → Rule=TAG_RESOURCE | Confidence=100 | dedicado

2. Tag FINOPS-CLIENTE no RG (≠ COMPARTILHADO, ≠ vazio)
   → Rule=TAG_RG_INHERITED | Confidence=95 | dedicado

3. Tag FINOPS-CLIENTE no RG = COMPARTILHADO
   → Rule=TAG_RG_SHARED | Confidence=90 | rateio

4. RG segue padrão MC_<rg>_<cluster>_<region>
   → Rule=OVERRIDE_RG_AUTO | Confidence=95 | mapeia para cluster AKS

5. Inferência pelo nome do RG
   → Rule=RG_INFERRED | Confidence=70 | compartilhado

6. Sem classificação
   → UNKNOWN | unallocated
```

Override pós-hierarquia (aplicado sobre o resultado das 6 regras acima):
- `ResourceType = microsoft.sql/servers/elasticpools`
  → `ServiceKey = SQLPOOL:{pool_name}` | `ServiceType = sql_pool`
  → Não é capturado por s03a (dedicado) — processado exclusivamente por s03c

---

## Clusters AKS

Arquivo: `config/reference/cluster_config.csv`

| Cluster | Resource Group | Tipo | Cliente | OpenCost |
|---|---|---|---|---|
| aks-prd-newkmm | RG-US-PRD-KMM | shared | — | ✅ http://10.152.26.15:9003 |
| aks-prd-br-newkmm | RG-PRD-KMM | shared | — | ✅ http://10.151.24.6:9003 |
| aks-hml-newkmm | (confirmar) | shared | — | ✅ http://10.152.12.117:9003 |
| aks-mg-kmm-firewall-eus-prd | RG-US-PRD-KMM | shared | — | ✅ http://10.152.51.4:9003 |
| aks-mg-kmm-firewall-eus-hml | RG-DEV-KMM | shared | — | ✅ http://10.153.6.6:9003 |
| aks-mg-kmm-dados-eus-prd | (confirmar) | shared | — | ❌ pendente instalação |
| aks-prd-emissor | (confirmar) | shared | — | ❌ pendente migração |
| aks-mg-kmm-win-eus-hml | (confirmar) | shared | — | ❌ pendente instalação |
| aks-mg-kmm-nocst-eus-prd-01 | (confirmar) | dedicated | NORCOAST | ❌ não necessário |
| aks-mg-kmm-nocst-eus-hml-01 | (confirmar) | dedicated | NORCOAST | ❌ não necessário |
| aks-mg-kmm-nocst-eus-qa-01 | (confirmar) | dedicated | NORCOAST | ❌ não necessário |

---

## Steps do pipeline e schedules

```
04:00 UTC           → s00_sync_rg_tags   (varre RGs das subscriptions → rg_tags.csv)
05:00 UTC           → s03b_aks           (dentro de cada cluster compartilhado)
06:00 UTC           → pipeline principal (s01→s02→s03a→s03c→s04→s05→s06→s07) — processa D-3 BRT
07:00 UTC (2ª-feira)→ finops-retro-fix   (reprocessa últimos 7 dias com FOCUS atualizado)
```

| Step | Arquivo | Descrição |
|---|---|---|
| s00 | `finops/steps/s00_sync_rg_tags.py` | Varre RGs, salva rg_tags.csv |
| s01 | `finops/steps/s01_ingest_focus.py` | Lê FOCUS parquet → fact_cost, filtra por BILLING_SUBSCRIPTION_IDS |
| s02 | `finops/steps/s02_classify.py` | Classifica recursos → resource_to_service |
| s03a | `finops/steps/s03a_dedicated.py` | Recursos dedicados, share=1.0 |
| s03b | `finops/steps/s03b_aks.py` | AKS via OpenCost |
| s03c | `finops/steps/s03c_sql_pool.py` | SQL Pool via CPU+Disk (ARM API + Azure Monitor) |
| s04 | `finops/steps/s04_client_weights.py` | Peso por cliente |
| s05 | `finops/steps/s05_consolidate.py` | Consolida shares, valida soma=1.0 |
| s06 | `finops/steps/s06_fact_allocated_cost.py` | AllocatedCost = BilledCost × Share, 5 outputs |
| s07 | `finops/steps/s07_consolidate_pbi.py` | Append diário no consolidado Power BI + manifest.csv |

---

## Convenções de código

- Separador CSV: `;`
- Encoding: UTF-8
- Decimais: 6 casas, ponto como separador
- Timezone: BRT = UTC-3 (sem DST)
- Particionamento Blob: `dt=yyyy-MM-dd`
- Autenticação: sempre `DefaultAzureCredential` — nunca hardcode de senha
- Logs: prefixos emoji (📅 ⬇️ ⬆️ ✅ ⚠ ❌)
- Retry: 3 tentativas com backoff exponencial em toda operação de I/O

---

## Variáveis de ambiente (ConfigMap Kubernetes / .env local)

```
# Storage FOCUS (entrada - somente leitura)
FOCUS_STORAGE_ACCOUNT=azurecostexportfocus10
FOCUS_SUBSCRIPTION_ID=0535039d-8b8e-4ac1-9fce-9f727e8ba2b1
FOCUS_CONTAINER=azurecostexportfocus10
FOCUS_PREFIX=costexport/azure-cost-export-focus10-52548376

# Subscriptions de faturamento cujos recursos são incluídos no pipeline
BILLING_SUBSCRIPTION_IDS=52d4423b-7ed9-4673-b8e2-fa21cdb83176,3f6d197f-f70b-4c2c-b981-8bb575d47a7a

# Storage Pipeline (saída)
PIPELINE_STORAGE_ACCOUNT=stpslkmmfinopseusprd
PIPELINE_SUBSCRIPTION_ID=52d4423b-7ed9-4673-b8e2-fa21cdb83176
PIPELINE_CONTAINER=finops-v2

# Prefixos internos
REFERENCE_PREFIX=config/reference
FACT_COST_PREFIX=silver/fact_cost
RESOURCE_TO_SERVICE_PREFIX=silver/resource_to_service
DEDICATED_OUT_PREFIX=gold/dedicated_allocation_share
AKS_OUT_PREFIX=gold/aks_allocation_share_opencost
SQL_POOL_OUT_PREFIX=gold/sql_pool_allocation_share
CLIENT_WEIGHTS_PREFIX=silver/client_weights
ALLOCATION_SHARE_FINAL_PREFIX=gold/allocation_share_final
FACT_ALLOCATED_COST_PREFIX=gold/fact_allocated_cost
PBI_CONSOLIDATED_BLOB=gold/fact_allocated_cost_consolidated/fact_allocated_cost_all.parquet
PBI_MANIFEST_BLOB=gold/fact_allocated_cost_consolidated/manifest.csv

# OpenCost (usado apenas no job AKS)
OPENCOST_ENDPOINT=http://opencost.opencost.svc:9003
OPENCOST_TIMEOUT_SEC=60
OPENCOST_RETRY_COUNT=2
OPENCOST_RETRY_DELAY_SEC=5

# Pipeline
PIPELINE_DATE=        # vazio = automático D-3 BRT (FOCUS_LAG_DAYS=3); para retrocarga manual: yyyy-MM-dd
LOG_LEVEL=INFO
```

---

## Schemas dos arquivos

### fact_cost
```
Date;ResourceId;ResourceName;ResourceGroupName;ResourceType;SubscriptionId;ServiceName;Cost
```

### rg_tags.csv
```
ResourceGroupName;FinopsCliente;SubscriptionId;UpdatedAt
rg-norcoast-prd;NORCOAST;3f6d197f-...;2026-04-22T04:00:00
```

### resource_to_service
```
Date;ResourceId;ResourceType;ResourceName;ResourceGroupName;
ServiceKey;ServiceType;Rule;Confidence;EffectiveFinopsCliente;
FinopsClienteSource;Notes
```

### allocation_share_final
```
Date;ServiceKey;Cliente;Share;DriverType;AllocationMethod
```

### fact_allocated_cost
```
Date;ResourceId;ServiceKey;Cliente;Cost;Share;AllocatedCost;AllocationMethod
```

### fact_allocated_cost_all.parquet (consolidado Power BI)
Mesmo schema do fact_allocated_cost — todas as datas em um único arquivo.
Colunas extras incluídas pelo s06: `ResourceName;ResourceGroupName;ResourceType;SubscriptionId;ServiceName;DriverType`

### manifest.csv (consolidado Power BI)
```
Date;Status;Rows;TotalAllocated;TotalCost;ProcessedAt;PipelineRun
```
- `Status`: `OK` ou `ERROR`
- `TotalAllocated`: soma de `AllocatedCost` do dia (custo real alocado)
- `TotalCost`: `TotalFactCost` da reconciliation do dia — custo bruto real do FOCUS sem duplicação
- `PipelineRun`: valor de `PIPELINE_RUN_ID` no momento do processamento

**Atenção — campo `Cost` no fact_allocated_cost:** o campo `Cost` (BilledCost bruto do recurso)
é repetido uma vez por cliente no join de fan-out do s06. `SUM(Cost)` no Power BI fica inflado
pelo número de clientes no pool proporcional. Usar sempre `SUM(AllocatedCost)` como métrica
de custo real — shares somam 1.0 por recurso, então AllocatedCost total == custo bruto real.

---

## Estado atual — tudo operacional ✅

### Código Python — 100% implementado
- [x] `finops/utils/dates.py` — inclui `brazil_today()` e `parse_usage_date()`
- [x] `finops/utils/logging.py`
- [x] `finops/storage.py` — dois storage accounts; `download_csv_with_fallback()`, `upload_parquet()`, `upload_csv()`
- [x] `finops/steps/s00_sync_rg_tags.py`
- [x] `finops/steps/s01_ingest_focus.py` — inclui filtro `BILLING_SUBSCRIPTION_IDS` com normalização de prefixo ARM; usa `EffectiveCost` em vez de `BilledCost` (RI amortizada embutida nas linhas Usage com RG correto); filtra `EffectiveCost>0` (remove Purchase/RI sem consumo)
- [x] `finops/steps/s02_classify.py` — hierarquia 6 regras, MC_ pattern, Tags JSON, vetorizado com np.select; elastic pools recebem `ServiceKey=SQLPOOL:{pool_name}` e `ServiceType=sql_pool`
- [x] `finops/steps/s03a_dedicated.py` — Share=1.0 por recurso dedicado
- [x] `finops/steps/s03b_aks.py` — AKS via OpenCost; namespaces `DEFAULT` e `COMPARTILHADO` redistribuídos proporcionalmente (não entram como cliente); gera `namespaces_sem_alias_{date}.csv` para auditoria
- [x] `finops/steps/s03c_sql_pool.py` — SQL Elastic Pools: ARM API + Monitor CPU; databases COMPARTILHADO redistribuídos proporcionalmente aos databases tagueados do mesmo pool (share calculado sobre `total_tagged_weighted`); gera `databases_sem_tag_{date}.csv` para auditoria
- [x] `finops/steps/s04_client_weights.py` — peso por custo dedicado, fallback igualitário
- [x] `finops/steps/s05_consolidate.py` — consolida s03a+s03b+s03c, distribui restante por client_weights; `_read_csv_partition` filtra CSVs por `include_prefix`/`exclude_patterns` para evitar leitura de arquivos de auditoria/detalhe que não têm `AllocationMethod`
- [x] `finops/steps/s06_fact_allocated_cost.py` — AllocatedCost = Cost × Share, 5 outputs por data; guard defensivo descarta e loga linhas residuais com `AllocationMethod=NaN`
- [x] `finops/steps/s07_consolidate_pbi.py` — append diário idempotente + manifest.csv; TotalCost lido da reconciliation (não do campo Cost inflado); validação (linhas + TotalAllocated diff < 1.0); manifest ERROR em falha
- [x] `jobs/run_aks.py`
- [x] `jobs/run_sync_rg_tags.py`
- [x] `jobs/run_pipeline.py` — s07 integrado como último step
- [x] `sh/retro_may.py` — retrocarga original 2026-05-01 a 2026-05-15
- [x] `sh/retro_fix_may15_20.py` — retrocarga corretiva pontual (obsoleto; substituído por retro_fix.py)
- [x] `sh/retro_fix.py` — retrocarga corretiva genérica; aceita `--start`/`--end` ou env vars `RETRO_START`/`RETRO_END` (compatível com Kubernetes Job); compara antes/depois; executa s01→s06 por data e faz consolidação PBI em **lote único ao final** (1 download + 1 upload do consolidado, em vez de N×download+upload); `_read_manifest_antes` e `_read_total_fact_cost` criam `pipeline_client()` fresco (evita token stale)
- [x] `sh/build_consolidated.py` — percorre todos os parquets em `gold/fact_allocated_cost/` e chama s07 por data
- [x] `requirements.txt` — `azure-monitor-query==1.3.0`, `azure-mgmt-sql>=3.0.0`

### Kubernetes / infra de código
- [x] `k8s/cronjob-aks.yaml` — um por cluster, CLIENT_ID preenchido
- [x] `k8s/cronjob-sync-rg-tags.yaml`
- [x] `k8s/cronjob-pipeline.yaml` — inclui `BILLING_SUBSCRIPTION_IDS` no ConfigMap
- [x] `k8s/job-fix-consolidated.yaml` — job avulso para re-executar build_consolidated (corretivo/retroativo)
- [x] `k8s/cronjob-retro-fix.yaml` — CronJob semanal (toda 2ª-feira 07:00 UTC) que executa `sh/retro_fix.py` (últimos 7 dias)
- [x] `k8s/job-retro.yaml` — Job one-shot para retrocarga manual com intervalo customizado via `RETRO_START`/`RETRO_END`
- [x] `Dockerfile` — inclui `COPY sh/ ./sh/` para expor build_consolidated.py nos pods
- [x] `requirements.txt`

### Configuração / referência
- [x] `config/reference/cluster_config.csv`
- [x] `config/reference/clientes_alias.csv`

### Infraestrutura provisionada
- [x] ACR: `registrykmm.azurecr.io/finops-pipeline:latest` — build publicado (cq11, 2026-05-26; inclui s03b DEFAULT fix + s03c COMPARTILHADO redistribuição + s05 CSV filter + s06 NaN guard)
- [x] Container `finops-v2` criado em `stpslkmmfinopseusprd`

### Clusters AKS — todos os 5 com OpenCost operacionais ✅

#### aks-hml-newkmm (2026-04-30)
- Identity: `id-mg-kmm-finops-aks-newkmm-eus-hml` (CLIENT_ID: `5eed8b89-7707-44cf-85e3-a6f86866a25f`)
- CronJob AKS aplicado — executa diariamente às 05:00 UTC

#### aks-prd-newkmm — cluster central (2026-04-30 / atualizado 2026-05-16)
- Identity: `id-mg-kmm-finops-aks-prd-newkmm-eus-prd` (CLIENT_ID: `40995235-e00b-417e-83a6-4ca383f9d169`)
- Permissões: `Storage Blob Data Contributor` em `finops-v2` + `Reader` em `52d4423b` + `Reader` em `3f6d197f` + `Storage Blob Data Reader` em `azurecostexportfocus10`
- `finops-sync-rg-tags` CronJob — 04:00 UTC ✅
- `finops-pipeline` CronJob — 06:00 UTC ✅
- Retrocarga original: 2026-05-01 a 2026-05-15 (15/15 OK, 100% alocado)
- Retrocarga corretiva: 2026-05-15 a 2026-05-20 (6/6 OK) — reprocessado com FOCUS atualizado (2026-05-21)
- Retrocarga pós-bugfix (2026-05-26): 24/24 OK — bugs SQLPOOL duplicatas + DEFAULT + COMPARTILHADO corrigidos

#### aks-prd-br-newkmm (2026-05-04)
- Identity: `id-mg-kmm-finops-aks-prd-br-newkmm-eus-prd` (CLIENT_ID: `e67c4ddc-1e3f-451b-a9bd-b41296bf2834`)
- CronJob AKS aplicado — executa diariamente às 05:00 UTC

#### aks-mg-kmm-firewall-eus-prd (2026-05-04)
- Subscription: `52d4423b-7ed9-4673-b8e2-fa21cdb83176` | RG: `RG-US-PRD-KMM`
- Identity: `id-mg-kmm-finops-aks-firewall-eus-prd` (CLIENT_ID: `23fe2b59-90a1-450d-8531-de76eba5340a`)
- CronJob AKS aplicado — executa diariamente às 05:00 UTC

#### aks-mg-kmm-firewall-eus-hml (2026-05-04)
- Subscription: `3f6d197f-f70b-4c2c-b981-8bb575d47a7a` | RG: `RG-DEV-KMM`
- Identity: `id-mg-kmm-finops-aks-firewall-eus-hml` (CLIENT_ID: `bffedfaa-5c53-4409-988c-c6884579cebc`)
- CronJob AKS aplicado — executa diariamente às 05:00 UTC

---

## Validação do pipeline (2026-05-26)

Pipeline validado end-to-end com escopo correto (apenas subscriptions KMM):

| Métrica | Valor |
|---|---|
| Escopo | `52d4423b` (KMM PRD) + `3f6d197f` (KMM DEV-TEST) |
| TotalAllocated | **100,0%** (todas as 24 datas) |
| Custo médio diário | ~R$ 13.100–15.200/dia (dados settlados, varia por dia) |
| Total maio (24 datas, 01–24) | **R$ 336.659** | média diária: **~R$ 14.027** |
| Projeção maio completo | ~R$ 434k (alinhado ao esperado ~R$ 430k) |
| Método predominante | WEIGHT_PROPORTIONAL + DEDICATED_TAG + AKS_OPENCOST |
| AKS_OPENCOST | Ativo — R$ ~1.500/dia via consumo real de namespace |
| SQLPOOL | Ativo — 7 pools; custo de DBs COMPARTILHADO redistribuído aos DBs tagueados do mesmo pool |
| EffectiveCost | Usado em vez de BilledCost — RI amortizada embutida nas linhas Usage; Purchase (EffectiveCost=0) filtrado |

5 outputs do s06 por data em `gold/fact_allocated_cost/dt=<data>/`:
- `fact_allocated_cost_<data>.parquet`
- `final_cost_by_client_<data>.parquet`
- `final_cost_detailed_<data>.parquet`
- `reconciliation_<data>.csv`
- `unallocated_cost_<data>.csv`

Consolidado Power BI — atualizado em 2026-05-26 (retrocarga completa pós-bugfix):
- `gold/fact_allocated_cost_consolidated/fact_allocated_cost_all.parquet`: **1.601.714 linhas**, 24 datas (2026-05-01 a 2026-05-24), Status=OK
- Redução de ~19.823 linhas vs build anterior (1.621.537) — remoção das duplicatas SQLPOOL
- Retrocarga 2026-05-01 a 2026-05-24: **24/24 ✅** — sem COMPARTILHADO, sem DEFAULT, sem NaN AllocationMethod
- `TotalCost` no manifest lido de `reconciliation_<data>.csv` (sem duplicação do fan-out)
- Total maio (24 datas): **R$ 336.659** | média diária: **R$ 14.027**
- Consolidação feita em lote único (`_rebuild_consolidated_batch`) — 1 download + 1 upload do consolidado

### FOCUS_LAG_DAYS = 3 — estratégia D-3 + retro semanal

O FOCUS Report é exportado mensalmente (`20260501-20260531`) e atualizado diariamente pela Azure.
Charges de um dia continuam aparecendo no FOCUS por **2–3 dias** após a data de uso (billing lag).

**Constante documentada** em `finops/utils/dates.py`:
```python
FOCUS_LAG_DAYS = 3  # billing lag confirmado em produção
```

**Estratégia implementada (2026-05-22):**
- Pipeline principal processa **D-3** (não mais D-1) → dados settlados na maioria dos casos
- CronJob `finops-retro-fix` (toda 2ª-feira 07:00 UTC) reprocessa os **últimos 7 dias** com FOCUS
  atualizado → cobre lags maiores: RI, Savings Plans, estornos, virada de mês
- Para retrocarga manual: `python sh/retro_fix.py --start YYYY-MM-DD --end YYYY-MM-DD`

**Impacto observado antes da correção:** ao rodar D-1, custo capturado era ~35% do valor real.
Após mudança para D-3, projeção mensal passou de ~R$ 130k para ~R$ 447k (alinhado ao esperado ~420k).

Retrocarga corretiva 2026-05-15 a 2026-05-20 executada em 2026-05-22 (6/6 OK):

| Data | TotalFactCost antes | TotalFactCost depois |
|---|---|---|
| 2026-05-15 | 4.726 | 12.408 (+163%) |
| 2026-05-16 | 4.883 | 12.752 (+161%) |
| 2026-05-17 | 4.672 | 12.693 (+171%) |
| 2026-05-18 | 4.825 | 12.945 (+168%) |
| 2026-05-19 | 5.004 | 13.342 (+167%) |
| 2026-05-20 | 4.338 | 12.861 (+197%) |

---

## Pendências

### Clusters sem OpenCost (bloquear — instalar OpenCost primeiro)
- [ ] aks-mg-kmm-dados-eus-prd — instalar OpenCost, depois seguir fluxo de identidade
- [ ] aks-prd-emissor — migrar para OpenCost, depois seguir fluxo de identidade
- [ ] aks-mg-kmm-win-eus-hml — instalar OpenCost, depois seguir fluxo de identidade

### ~~BUG CRÍTICO — AKS_OPENCOST = R$ 0~~ — RESOLVIDO (2026-05-25) ✅

**Causa raiz identificada e corrigida:** `s02_classify.py` linha 187 — `rule4_wins` tinha `& ~cond3` que bloqueava OVERRIDE_RG_AUTO quando MC_* RGs tinham tag COMPARTILHADO no rg_tags.csv.

**Fix aplicado:** removido `& ~cond3` da condição `rule4_wins`. OVERRIDE_RG_AUTO agora tem prioridade sobre TAG_RG_SHARED para RGs MC_*.
Nota: `Rule` no r2s ainda mostra `TAG_RG_SHARED` (np.select não foi alterado), mas `ServiceKey=AKS:{cluster}` está correto — s06 usa ServiceKey para o join, não Rule.

**Validação em 2026-05-22:**
- AKS_OPENCOST: R$ 1.503,50 (era R$ 0) — 12.575 linhas, 4 clusters
- WEIGHT_PROPORTIONAL: R$ 1.986 (reduziu conforme esperado)
- TotalAllocated: 100% mantido

**Imagem publicada:** `registrykmm.azurecr.io/finops-pipeline:latest` (build cq11, 2026-05-26)
**Retrocarga 2026-05-01 a 2026-05-24 concluída (24/24 ✅, 2026-05-26)** — consolidado atualizado pós-bugfix.

### ~~3 bugs de alocação~~ — RESOLVIDOS (2026-05-26) ✅

**BUG 1 — SQLPOOL: 19.058 linhas duplicadas com `AllocationMethod=NaN`** (R$64k / 19% do total)
- **Causa raiz:** `s05._read_csv_partition` lia TODOS os CSVs do prefix sql_pool e aks, incluindo arquivos de auditoria/detalhe que não têm coluna `AllocationMethod`. O pd.concat injetava NaN nessa coluna no `allocation_share_final`.
- **Fix:** `_read_csv_partition` aceita `include_prefix` e `exclude_patterns`; SQL filtrado para `sqlpool_allocation_share_all_*`, AKS exclui `namespaces_sem_alias_*`. Guard em s06 descarta residuais com log `⚠ Removendo N linhas sem AllocationMethod`.

**BUG 2 — AKS: Cliente `DEFAULT` (R$4.824 / 1,4%)**
- **Causa raiz:** namespaces resolvidos via `clientes_alias.csv` como `DEFAULT` passavam o filtro `!= "COMPARTILHADO"` e entravam em `client_costs` como cliente real.
- **Fix:** s03b adiciona `"DEFAULT"` à lista de exclusão; esses namespaces vão para `unmapped_cost` (redistribuição proporcional). Gera `namespaces_sem_alias_{date}.csv` para auditoria.

**BUG 3 — SQL: Cliente `COMPARTILHADO` residual (R$673 / 0,2%)**
- **Causa raiz:** s03c adicionava databases sem tag como `Cliente=COMPARTILHADO` em `all_share`, produzindo linha de custo real no output.
- **Fix:** s03c separa `tagged_rows` de `compartilhado_rows`; shares calculados sobre `total_tagged_weighted` (denominador exclui COMPARTILHADO, absorvendo proporcional). COMPARTILHADO vai apenas para `sqlpool_detail_all_` com `Share=0`.

### Qualidade de dados (melhorias incrementais)
- [x] ~~Taguear databases SQL Elastic Pool com `FINOPS-CLIENTE`~~ — concluído (2026-05-24); s03c redistribui custo de databases COMPARTILHADO proporcionalmente; `databases_sem_tag_{date}.csv` gerado para auditoria
- [x] ~~Reserved Instances de VMs AKS~~ — resolvido via `EffectiveCost` no s01 (2026-05-24). EffectiveCost distribui o custo de RI de volta para os recursos com RG correto (incluindo MC_* dos clusters AKS). Purchase lines (EffectiveCost=0) removidas pelo filtro `EffectiveCost>0`. Arquivo `ri_cluster_map.csv` não necessário.
- [x] ~~Tags numéricas~~ — investigado em 2026-05-25; zero ocorrências no consolidado de maio. Não confirmado nos dados reais — item encerrado.

### Relatórios / Power BI
- [x] Criar arquivo consolidado `gold/fact_allocated_cost_consolidated/fact_allocated_cost_all.parquet` — **1.601.714 linhas**, 24 datas (2026-05-01 a 2026-05-24) ✅
- [x] manifest.csv com status por data — TotalCost corrigido (sem inflação do fan-out) ✅
- [ ] Power BI — conectar direto no Blob Storage (`finops-v2`) via Azure Storage connector e apontar para o consolidado
- [x] **Pipeline processa D-3** — `FOCUS_LAG_DAYS=3` em `finops/utils/dates.py`; log: `📅 Pipeline date: YYYY-MM-DD (D-3, FOCUS lag=3 dias)` ✅
- [x] **CronJob `finops-retro-fix`** — toda 2ª-feira 07:00 UTC, reprocessa últimos 7 dias via `sh/retro_fix.py` ✅

---

## s03c — lógica de alocação SQL Elastic Pool

1. Filtra `Microsoft.Sql/servers/elasticPools` não-dedicados em `resource_to_service`
2. Para cada pool → `GET .../elasticPools/{pool}/databases` (ARM API)
3. Para cada database → CPU média via Azure Monitor (`cpu_percent`, agregação diária)
4. Mapeia database → cliente via tag `FINOPS-CLIENTE` no database
5. Databases com tag COMPARTILHADO (ou sem tag) → WeightedUsage redistribuído proporcionalmente aos clientes tagueados do mesmo pool; não geram linha em `allocation_share_final`
6. `Share = WeightedUsage_cliente / total_tagged_weighted_pool`
7. Fallback: CPU = 0 recebe peso mínimo `0.01` para não ser descartado silenciosamente

**Permissões necessárias:** `Reader` nas subscriptions (já atribuído) — suficiente para ARM list e Monitor metrics.

**Status atual (2026-05-22):** s03c ativo e integrado ao pipeline. Elastic pools recebem `ServiceKey=SQLPOOL:{pool_name}` e `ServiceType=sql_pool` em s02. Quando pools aparecem no FOCUS, s03c processa via ARM API + Azure Monitor. Databases sem tag `FINOPS-CLIENTE` entram como `COMPARTILHADO` no rateio proporcional (não mais como `DEFINIR`/REDISTRIBUTED_TO_CLIENTS). Audit CSV `databases_sem_tag_{date}.csv` gerado em `gold/sql_pool_allocation_share/dt={date}/` — em maio/2026, ~18 databases sem tag em 4 pools por data.

---

## Cluster central — aks-prd-newkmm

Este cluster roda os jobs centralizados (não replicados em outros clusters):
- `finops-sync-rg-tags` (04:00 UTC) — s00 ✅
- `finops-pipeline` (06:00 UTC) — pipeline principal ✅

A identity `id-mg-kmm-finops-aks-prd-newkmm-eus-prd` acumulou todas as permissões necessárias:
- `Storage Blob Data Contributor` em `finops-v2` ✅
- `Reader` em `52d4423b` (KMM PRD) e `3f6d197f` (KMM DEV-TEST) ✅
- `Storage Blob Data Reader` em `azurecostexportfocus10` ✅

---

## Comandos úteis

### Ambiente local
```bash
python -m venv .venv
source .venv/bin/activate        # Linux/Mac
.venv\Scripts\activate           # Windows
pip install -r requirements.txt
```

### Build da imagem
```bash
az acr build --registry registrykmm --image finops-pipeline:latest .
```

### Conectar no cluster e aplicar manifests
```bash
az aks get-credentials --resource-group <RG> --name <CLUSTER>
kubectl create namespace finops --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/cronjob-aks.yaml
```

### Teste manual — pipeline principal
```bash
az aks get-credentials --resource-group RG-US-PRD-KMM --name aks-prd-newkmm --overwrite-existing
kubectl create job --from=cronjob/finops-pipeline finops-pipeline-test-$(date +%s) -n finops
kubectl logs -f job/<nome-gerado> -n finops
```

### Retrocarga corretiva — reprocessar dias recentes com FOCUS atualizado
```bash
# Editar DATES em sh/retro_fix_may15_20.py e executar
python sh/retro_fix_may15_20.py

# Após retrocarga corretiva, reconstruir consolidado Power BI
kubectl delete job finops-fix-consolidated -n finops --ignore-not-found=true
kubectl apply -f k8s/job-fix-consolidated.yaml
kubectl wait --for=condition=complete --timeout=600s job/finops-fix-consolidated -n finops
kubectl logs job/finops-fix-consolidated -n finops | grep -E "(s07 —|concluida)"
```

### Teste manual — data específica (retrocarga)
```bash
# Via script Python (sequencial, todas as datas de uma vez)
python sh/retro_may.py

# Via kubectl com override de data (uma data)
kubectl get cronjob finops-pipeline -n finops -o json | python3 -c "
import sys, json
d = json.load(sys.stdin)
spec = d['spec']['jobTemplate']['spec']
for c in spec['template']['spec']['containers']:
    if c['name'] == 'finops-pipeline':
        c.setdefault('env', []).append({'name': 'PIPELINE_DATE', 'value': '2026-05-10'})
print(json.dumps({'apiVersion':'batch/v1','kind':'Job','metadata':{'name':'finops-retro-test','namespace':'finops'},'spec':spec}))
" | kubectl apply -f -
```

### Enviar config/reference para o Blob
```bash
for f in cluster_config.csv clientes_alias.csv aks_system_namespaces.csv; do
  az storage blob upload \
    --account-name stpslkmmfinopseusprd \
    --container-name finops-v2 \
    --name "config/reference/$f" \
    --file "config/reference/$f" \
    --auth-mode login
done
```

### Validar permissões da Managed Identity
```bash
# Reader no FOCUS storage
az role assignment list \
  --assignee <OBJECT-ID> \
  --scope "/subscriptions/0535039d-8b8e-4ac1-9fce-9f727e8ba2b1/resourceGroups/azurecostexportfocus10/providers/Microsoft.Storage/storageAccounts/azurecostexportfocus10" \
  --query "[].roleDefinitionName" -o tsv

# Contributor no pipeline storage
az role assignment list \
  --assignee <OBJECT-ID> \
  --scope "/subscriptions/52d4423b-7ed9-4673-b8e2-fa21cdb83176/resourceGroups/rg-psl-kmm-finops-eus-prd/providers/Microsoft.Storage/storageAccounts/stpslkmmfinopseusprd/blobServices/default/containers/finops-v2" \
  --query "[].roleDefinitionName" -o tsv
```
