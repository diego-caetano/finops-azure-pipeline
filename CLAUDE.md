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
    clientes_alias.csv           ← alias namespace/nome → cliente (89 entradas)
    aks_system_namespaces.csv    ← namespaces de sistema a redistribuir
    overrides_rg_auto.csv        ← padrões MC_ gerados automaticamente
    rg_tags.csv                  ← tags FINOPS-CLIENTE dos RGs (gerado diário)
    backup_vaults.csv            ← vaults a processar pelo s03d (VaultName, ResourceGroup, SubscriptionId, ServiceKey)

  silver/fact_cost/dt=yyyy-MM-dd/
  silver/resource_to_service/dt=yyyy-MM-dd/
  silver/client_weights/dt=yyyy-MM-dd/

  gold/aks_allocation_share_opencost/dt=yyyy-MM-dd/
  gold/sql_pool_allocation_share/dt=yyyy-MM-dd/
    sqlpool_detail_all_<date>.csv
    sqlpool_allocation_share_all_<date>.csv
    databases_sem_tag_<date>.csv      ← databases sem tag FINOPS-CLIENTE (alocados como COMPARTILHADO)
  gold/backup_allocation_share/dt=yyyy-MM-dd/
    backup_share_<date>.csv           ← shares de backup para s05 (snapshots + vaults)
    vault_detail_<date>.csv           ← auditoria de protected items por vault
    snapshots_sem_alias_<date>.csv    ← snapshots sem alias mapeável (ficam em WEIGHT_PROPORTIONAL)
  gold/allocation_share_final/dt=yyyy-MM-dd/
  gold/fact_allocated_cost/dt=yyyy-MM-dd/
  gold/fact_allocated_cost_consolidated/
    fact_allocated_cost_YYYY-MM.parquet  ← um arquivo por mês para BI (selado ao virar o mês)
    manifest.csv                          ← uma linha por data (Status, Rows, TotalAllocated) — todos os meses
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
- `ResourceType = microsoft.recoveryservices/vaults`
  → `ServiceKey = VAULT:{ResourceName}` | `ServiceType = backup_vault`
  → Processado por s03d — rateio por protected items via ARM API
- `ResourceType = microsoft.compute/snapshots`
  → `ServiceType = snapshot` (ServiceKey permanece ResourceId)
  → Processado por s03d — mapeamento direto por ResourceName → clientes_alias

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
06:00 UTC           → pipeline principal (s01→s02→s03a→s03c→s03d→s04→s05→s06→s07→s08) — processa D-3 BRT
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
| s03d | `finops/steps/s03d_backup.py` | Backup e Snapshots: snapshot→alias direto, vault→protected items ARM API |
| s04 | `finops/steps/s04_client_weights.py` | Peso por cliente |
| s05 | `finops/steps/s05_consolidate.py` | Consolida shares, valida soma=1.0 |
| s06 | `finops/steps/s06_fact_allocated_cost.py` | AllocatedCost = BilledCost × Share, 5 outputs |
| s07 | `finops/steps/s07_consolidate_pbi.py` | Append diário no consolidado Power BI + manifest.csv |
| s08 | `finops/steps/s08_quality_check.py` | Alertas de qualidade: namespaces sem alias + recursos não alocados |

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
PBI_CONSOLIDATED_PREFIX=gold/fact_allocated_cost_consolidated
PBI_MANIFEST_BLOB=gold/fact_allocated_cost_consolidated/manifest.csv

# OpenCost (usado apenas no job AKS)
OPENCOST_ENDPOINT=http://opencost.opencost.svc:9003
OPENCOST_TIMEOUT_SEC=60
OPENCOST_RETRY_COUNT=2
OPENCOST_RETRY_DELAY_SEC=5

# Backup/Snapshots
BACKUP_OUT_PREFIX=gold/backup_allocation_share

# Pipeline
PIPELINE_DATE=        # vazio = automático D-3 BRT (FOCUS_LAG_DAYS=3); para retrocarga manual: yyyy-MM-dd
LOG_LEVEL=INFO

# Alertas de qualidade (s08)
QUALITY_ALERTS_PREFIX=gold/quality_alerts
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

**Mutabilidade dos arquivos diários:** o arquivo `fact_allocated_cost_<date>.parquet` em
`gold/fact_allocated_cost/dt=<date>/` **não é imutável**. Ele é sobrescrito:
1. **Primeiro processamento (D+3):** pipeline normal processa D-3 BRT — primeira versão do arquivo.
2. **retro_fix (toda 2ª-feira):** reprocessa os últimos 7 dias com FOCUS mais settlado — sobrescreve com dados corrigidos.

**Janela de estabilização de um arquivo diário:**
- Arquivo para `YYYY-MM-DD` é criado em `DD+3` (pipeline D-3)
- Considerado **estável após o retro_fix da semana seguinte** (~DD+10)
- Para o time de dados: consumir após a segunda-feira seguinte à data garante a versão mais precisa

**Fechamento do mês:** o arquivo mensal `fact_allocated_cost_YYYY-MM.parquet` recebe os últimos
dias do mês nos primeiros dias do mês seguinte via pipeline D-3:
- 29/05 → processado em 01/06 | 30/05 → 02/06 | 31/05 → 03/06

O retro_fix da primeira segunda-feira do mês (ex: 01/06) cobre os últimos 7 dias (25–31/05)
mas com FOCUS T+1 — ainda incompleto. A versão definitiva de cada dia vem do **pipeline D-3**
(ex: maio/31 é definitivo após 03/06). O retro_fix da segunda-feira seguinte (ex: 08/06) já
não cobre mais dias de maio (cobre 01–07/06). O mês está **definitivamente fechado após D+3
do último dia** (ex: maio fecha em 03/06).

### fact_allocated_cost_YYYY-MM.parquet (consolidado mensal Power BI)
Mesmo schema do fact_allocated_cost — todas as datas do mês em um único arquivo por mês.
Colunas extras incluídas pelo s06: `ResourceName;ResourceGroupName;ResourceType;SubscriptionId;ServiceName;DriverType`
O arquivo mensal sofre as mesmas atualizações dos diários (D-3 + retro_fix) até ser fechado.
`gerar_dashboard_5.py` usa env var `DASHBOARD_MONTH=YYYY-MM` para selecionar o mês (padrão: mês atual).

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
- [x] `finops/steps/s02_classify.py` — hierarquia 6 regras, MC_ pattern, Tags JSON, vetorizado com np.select; elastic pools → `ServiceKey=SQLPOOL:{pool_name}` + `ServiceType=sql_pool`; vaults → `ServiceKey=VAULT:{ResourceName}` + `ServiceType=backup_vault`; snapshots → `ServiceType=snapshot`
- [x] `finops/steps/s03a_dedicated.py` — Share=1.0 por recurso dedicado
- [x] `finops/steps/s03b_aks.py` — AKS via OpenCost; namespaces `DEFAULT` e `COMPARTILHADO` redistribuídos proporcionalmente (não entram como cliente); gera `namespaces_sem_alias_{date}.csv` para auditoria
- [x] `finops/steps/s03c_sql_pool.py` — SQL Elastic Pools: ARM API + Monitor CPU; databases COMPARTILHADO redistribuídos proporcionalmente aos databases tagueados do mesmo pool (share calculado sobre `total_tagged_weighted`); gera `databases_sem_tag_{date}.csv` para auditoria
- [x] `finops/steps/s03d_backup.py` — Azure Backup e Snapshots; Parte 1: snapshot→ResourceName→clientes_alias (BACKUP_SNAPSHOT_DIRECT, Share=1.0); Parte 2: vault→ARM backupProtectedItems→item count por cliente (BACKUP_VAULT_ITEMS); fallback WEIGHT_PROPORTIONAL se ARM falhar; gera `vault_detail_{date}.csv` e `snapshots_sem_alias_{date}.csv` para auditoria
- [x] `finops/steps/s04_client_weights.py` — peso por custo dedicado, fallback igualitário
- [x] `finops/steps/s05_consolidate.py` — consolida s03a+s03b+s03c+s03d, distribui restante por client_weights; `_read_csv_partition` filtra CSVs por `include_prefix`/`exclude_patterns`; colapsa df_aks por `(Date, ServiceKey, Cliente)`; filtra linhas `Cliente=DEFAULT` de CSVs históricos pré-cq11; lê `backup_share_*` do prefix `BACKUP_OUT_PREFIX`
- [x] `finops/steps/s06_fact_allocated_cost.py` — AllocatedCost = Cost × Share, 5 outputs por data; agrega fact_cost por `(Date, ResourceId, ServiceKey)` somando Cost ANTES do join com shares (elimina meter-level fan-out); guard NaN AllocationMethod; guard shares duplicados
- [x] `finops/steps/s07_consolidate_pbi.py` — append diário idempotente + manifest.csv; TotalCost lido da reconciliation (não do campo Cost inflado); validação (linhas + TotalAllocated diff < 1.0); manifest ERROR em falha; schema alignment antes do pd.concat (schema drift protection)
- [x] `finops/steps/s08_quality_check.py` — alertas de qualidade: agrega namespaces_sem_alias_* de todos os clusters + filtra unallocated_cost por reasons actionables (NO_SERVICEKEY, HAS_SERVICEKEY_NO_SHARE); log ⚠ + upload CSV para gold/quality_alerts/dt={date}/
- [x] `jobs/run_aks.py`
- [x] `jobs/run_sync_rg_tags.py`
- [x] `jobs/run_pipeline.py` — ordem: s01→s02→s03a→s03c→s03d→s04→s05→s06→s07→s08; try/except por step com `logger.exception()` — loga `❌ <step> falhou date=<data>` com traceback completo
- [x] `sh/retro_may.py` — retrocarga original 2026-05-01 a 2026-05-15
- [x] `sh/retro_fix_may15_20.py` — retrocarga corretiva pontual (obsoleto; substituído por retro_fix.py)
- [x] `sh/retro_fix.py` — retrocarga corretiva genérica; aceita `--start`/`--end` ou env vars `RETRO_START`/`RETRO_END` (compatível com Kubernetes Job); compara antes/depois; executa s01→s06 por data e faz consolidação PBI em **lote único ao final**; flag `SKIP_CONSOLIDATED_REBUILD=1` para modo validação (pula rebuild do consolidado); try/except por step com `logger.exception()`
- [x] `sh/build_consolidated.py` — percorre todos os parquets em `gold/fact_allocated_cost/` e chama s07 por data
- [x] `requirements.txt` — `azure-monitor-query==1.3.0`, `azure-mgmt-sql>=3.0.0`

### Kubernetes / infra de código
- [x] `k8s/cronjob-aks.yaml` — um por cluster, CLIENT_ID preenchido; anotações Grafana (`logs_enabled=true`, `logs_index=kmm-finops-newkmm-prd`)
- [x] `k8s/cronjob-sync-rg-tags.yaml` — anotações Grafana
- [x] `k8s/cronjob-pipeline.yaml` — inclui `BILLING_SUBSCRIPTION_IDS` no ConfigMap; anotações Grafana
- [x] `k8s/cronjob-retro-fix.yaml` — CronJob semanal (toda 2ª-feira 07:00 UTC) que executa `sh/retro_fix.py` (últimos 7 dias); anotações Grafana
- [x] `k8s/job-fix-consolidated.yaml` — job avulso para re-executar build_consolidated (corretivo/retroativo)
- [x] `k8s/job-retro.yaml` — Job one-shot para retrocarga manual com intervalo customizado via `RETRO_START`/`RETRO_END`
- [x] `Dockerfile` — inclui `COPY sh/ ./sh/` para expor build_consolidated.py nos pods
- [x] `requirements.txt`

### Configuração / referência
- [x] `config/reference/cluster_config.csv`
- [x] `config/reference/clientes_alias.csv`
- [x] `config/reference/backup_vaults.csv` — vaults a processar pelo s03d (VaultName, ResourceGroup, SubscriptionId, ServiceKey)

### Infraestrutura provisionada
- [x] ACR: `registrykmm.azurecr.io/finops-pipeline:latest` — build publicado (cq1b, 2026-06-09; fix s03c pool dedicado → DEDICATED_TAG)
- [x] Container `finops-v2` criado em `stpslkmmfinopseusprd`
- [x] Repositório GitHub: `https://github.com/caetanod/finops-azure-pipeline` (privado)

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
- Retrocarga pós-bugfix cq11 (2026-05-26): 24/24 OK — bugs SQLPOOL duplicatas + DEFAULT + COMPARTILHADO corrigidos
- Retrocarga pós-bugfix cq12 (2026-05-26): 24/24 OK — bug AKS fan-out inflation corrigido; consolidado: 1.596.542 linhas
- Retrocarga cq13 (2026-05-26): 25/25 OK — inclui 2026-05-25; consolidado: 1.675.919 linhas | R$ 351.553
- Retrocarga cq14 (2026-05-27): 25/25 OK — meter-level fan-out + DEFAULT histórico corrigidos; consolidado: **308.379 linhas** (−82%) | R$ 351.553
- Retrocarga cq18 validação (2026-05-28): 3/3 OK (2026-05-01 a 03) — s03d validado; BACKUP_VAULT_ITEMS: R$76,73/dia (2 vaults); vault em WEIGHT_PROPORTIONAL: 0
- Retrocarga cq18 completa (2026-05-28): **25/25 OK** — consolidado: **707.805 linhas** (25 datas) | R$ 351.552,92
- Retrocarga cq19 completa (2026-06-09): **31/31 OK** — consolidado: **905.849 linhas** (maio completo 01–31) | fix s03c pool dedicado MARONI (SQLPOOL_CPU → DEDICATED_TAG)

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

## Validação do pipeline (2026-06-09, cq19 — maio completo)

Pipeline validado end-to-end com escopo correto (apenas subscriptions KMM):

| Métrica | Valor |
|---|---|
| Escopo | `52d4423b` (KMM PRD) + `3f6d197f` (KMM DEV-TEST) |
| TotalAllocated | **100,0%** (todas as 31 datas) |
| Custo médio diário | ~R$ 13.400–15.700/dia (dados settlados) |
| Total maio (31 datas, 01–31) | **~R$ 458K** (estimativa) | média diária: **~R$ 14.780** |
| AKS_OPENCOST | Ativo — R$ ~1.500/dia via consumo real de namespace |
| SQLPOOL | Ativo — 7 pools; 1 pool dedicado (MARONI) → DEDICATED_TAG; 6 pools compartilhados → SQLPOOL_CPU por CPU |
| EffectiveCost | Usado em vez de BilledCost — RI amortizada embutida nas linhas Usage; Purchase (EffectiveCost=0) filtrado |

5 outputs do s06 por data em `gold/fact_allocated_cost/dt=<data>/`:
- `fact_allocated_cost_<data>.parquet`
- `final_cost_by_client_<data>.parquet`
- `final_cost_detailed_<data>.parquet`
- `reconciliation_<data>.csv`
- `unallocated_cost_<data>.csv`

Consolidado Power BI — atualizado em 2026-06-09 (retrocarga cq19: maio completo 01–31):
- `gold/fact_allocated_cost_consolidated/fact_allocated_cost_2026-05.parquet`: **905.849 linhas**, 31 datas (2026-05-01 a 2026-05-31), Status=OK
- `TotalCost` no manifest lido de `reconciliation_<data>.csv` (sem duplicação do fan-out)
- Consolidação feita em lote único (`_rebuild_consolidated_batch`) — 1 download + 1 upload do consolidado
- **Arquivo antigo `fact_allocated_cost_all.parquet` está obsoleto** — Power BI deve apontar para `fact_allocated_cost_2026-05.parquet` (ou usar folder connector dinâmico — ver docs/POWER_BI.md)

| AllocationMethod | AllocatedCost (maio, cq19) | Obs |
|---|---|---|
| DEDICATED_TAG | aumentou vs cq18 | inclui agora pool dedicado MARONI (`sqldb-elastic-pool-vcore-prd-maroni-cw`) |
| SQLPOOL_CPU | reduziu vs cq18 | apenas pools compartilhados; pool MARONI saiu deste bucket |
| WEIGHT_PROPORTIONAL | ~R$ 37.950 | vaults em BACKUP_VAULT_ITEMS |
| AKS_OPENCOST | ~R$ 33.116 | namespace por cluster |
| BACKUP_VAULT_ITEMS | ~R$ 1.917 | kmm-prd-default-bvault-us + vault-m41c2l5l |
| BACKUP_SNAPSHOT_DIRECT | R$ 0 | snapshots de maio tagueados → s03a |

**Reconciliação MARONI (cq19):**
- Azure Cost Management (tag=MARONI, maio): R$ 62.634
- Pipeline DEDICATED_TAG (após cq19): ~R$ 61.713 (recursos diretos + pool dedicado)
- Diferença residual ~R$ 921: EffectiveCost vs BilledCost (amortização RI) ✅

*Totais exatos via Power BI após atualizar fonte para `fact_allocated_cost_2026-05.parquet`.*

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

### s03d — Backup e Snapshots ✅ operacional (cq18, 2026-05-28)
- [x] Build cq18: `registrykmm.azurecr.io/finops-pipeline:latest` publicado (2026-05-28)
- [x] Upload de `backup_vaults.csv` para o Blob — 2 vaults: `kmm-prd-default-bvault-us` (PRD) + `vault-m41c2l5l` (DEV)
- [x] Validação end-to-end 2026-05-01 a 2026-05-03 — todos os 5 critérios passando; vault em WEIGHT_PROPORTIONAL: 0
- [x] Retrocarga completa maio (2026-05-01 a 2026-05-25) com cq18 — 25/25 OK (2026-05-28); consolidado 707.805 linhas | R$ 351.552,92

### ~~s03c — Pool SQL dedicado com AllocationMethod errado~~ — RESOLVIDO (cq19, 2026-06-09) ✅

**Problema:** pool `sqldb-elastic-pool-vcore-prd-maroni-cw` tem tag `FINOPS-CLIENTE=MARONI` (TAG_RESOURCE, Confidence=100), mas s03c ignorava `EffectiveFinopsCliente` e processava o pool via métricas de databases. Custo saía como `SQLPOOL_CPU`, ocultando R$ 49K de custo dedicado MARONI no Power BI ("Custo Dedicado" = R$ 12K em vez de R$ 62K).

**Fix (s03c_sql_pool.py):** antes de chamar ARM API, verifica `EffectiveFinopsCliente` do pool no r2s. Se dedicado → `Share=1.0`, `AllocationMethod=DEDICATED_TAG`, `DriverType=SQLPOOL_DEDICATED_POOL`, sem chamadas de API. Pools compartilhados continuam com lógica CPU normal.

**Build cq1b:** `registrykmm.azurecr.io/finops-pipeline:latest` publicado (2026-06-09)
**Retrocarga cq19 (2026-06-09): 31/31 OK** — maio completo 01–31; consolidado **905.849 linhas**
**Reconciliação MARONI:** DEDICATED_TAG ~R$ 61.713 ≈ Azure Cost Management R$ 62.634 (Δ ~R$ 921 = RI amortização) ✅

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

**Imagem publicada:** `registrykmm.azurecr.io/finops-pipeline:latest` (build cq12, 2026-05-26)
**Retrocarga 2026-05-01 a 2026-05-24 concluída (24/24 ✅, 2026-05-26)** — consolidado atualizado pós-bugfix (cq11 + cq12).

### ~~BUG CRÍTICO — AKS fan-out inflation~~ — RESOLVIDO (2026-05-26) ✅

**Problema:** recursos AKS sem namespace próprio (VMSS, Disks, LB, PVCs) recebiam N linhas por cliente (onde N = número de namespaces ativos), inflando AllocatedCost ~5x. Exemplo: `aks-poolprdkmm2-vmss` gerava 208 linhas/cliente; custo real R$192,99 vs AllocatedCost R$958,31.

**Causa raiz:** CSVs históricos de s03b por namespace (1 linha por namespace em vez de 1 linha por cliente) eram lidos por s05 e concatenados ao `allocation_share_final` com múltiplos pares `(ServiceKey, Cliente)` duplicados. O join fan-out em s06 multiplicava as linhas do fact_cost.

**Fix cq12 (defesa em profundidade):**
- `s05_consolidate.py`: colapsa df_aks por `(Date, ServiceKey, Cliente)` após leitura
- `s06_fact_allocated_cost.py`: guard antes do join colapsa pares `(ServiceKey, Cliente)` duplicados

**Resultado cq12 (2026-05-26):** consolidado reduziu de 1.601.714 → 1.596.542 linhas (−5.172). Total R$336.659,40 mantido.

### ~~BUG CRÍTICO — Meter-level fan-out (s06) + DEFAULT histórico (s05)~~ — RESOLVIDO (2026-05-27, cq14) ✅

**PROBLEMA 1 — s06 meter fan-out:**
- **Evidência:** `aks-poolprdkmm2-24841847-vmss` em 2026-05-01: 224 linhas FOCUS × 34 clientes = 7.072 linhas em vez de 34. AllocatedCost correto (R$514,90), mas parquet inflado 208×.
- **Causa raiz:** `fact_cost` tem N linhas por ResourceId (1 por meter/billing line do FOCUS). O join com `df_shares` (C clientes) produzia N×C linhas. O s05/s06 dedup do cq12 colapsava os *shares*, mas o *fact_cost* permanecia com N linhas por recurso.
- **Fix em s06:** agrega `merged_with_sk` por `(Date, ResourceId, ServiceKey)` somando Cost e usando `first()` para metadata, ANTES do join fan-out com shares. Resultado: 1 linha por (ResourceId × cliente).

**PROBLEMA 2 — DEFAULT em CSVs históricos:**
- **Evidência:** 17.403 linhas `Cliente=DEFAULT` (VMSS, Disks, LB em MC_* RGs) em todo o consolidado cq13.
- **Causa raiz:** CSVs de s03b escritos antes do fix cq11 tinham `Cliente=DEFAULT`. O retro_fix.py não re-executa s03b — ele lê os CSVs históricos do blob. O s05 consumia esses CSVs sem filtrar DEFAULT.
- **Fix em s05:** filtra `Cliente=DEFAULT` de `df_aks` após dedup, com warning log.
- **Fix defensivo em s03b:** guard remove DEFAULT do output caso apareça (no-op para código atual, mas protege edge cases futuros).

**Resultado cq14 (2026-05-27, validado 3 dias + retrocarga completa):**

| Métrica | Antes (cq13) | Depois (cq14) |
|---|---|---|
| Linhas consolidado | 1.675.919 | **308.379** (−82%) |
| VMSS linhas (maio/01) | 7.072 (224×31) | **33** |
| DEFAULT | 17.403 | **0** |
| AllocatedCost total | R$ 351.553 | **R$ 351.553** (idêntico) |

---

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
- [x] Criar arquivo consolidado `gold/fact_allocated_cost_consolidated/fact_allocated_cost_all.parquet` — **308.379 linhas**, 25 datas (2026-05-01 a 2026-05-25) ✅ (cq14)
- [x] manifest.csv com status por data — TotalCost corrigido (sem inflação do fan-out) ✅
- [ ] Power BI — conectar direto no Blob Storage (`finops-v2`) via Azure Storage connector e apontar para o consolidado
- [x] **Pipeline processa D-3** — `FOCUS_LAG_DAYS=3` em `finops/utils/dates.py`; log: `📅 Pipeline date: YYYY-MM-DD (D-3, FOCUS lag=3 dias)` ✅
- [x] **CronJob `finops-retro-fix`** — toda 2ª-feira 07:00 UTC, reprocessa últimos 7 dias via `sh/retro_fix.py` ✅

---

## s03d — lógica de alocação Azure Backup e Snapshots

### Parte 1 — Snapshots (microsoft.compute/snapshots)

1. Filtra r2s por `ResourceType = microsoft.compute/snapshots` excluindo linhas com `Rule = TAG_RESOURCE/TAG_RG_INHERITED` (já tratadas por s03a)
2. Para cada snapshot, resolve o cliente em 3 etapas:
   - Extrai segmento de VM: `vm-{segmento}-prd-...` → `{segmento}` → busca em `clientes_alias.csv` (exact + substring)
   - Fallback: nome completo do recurso → busca em `clientes_alias.csv`
   - Fallback: RG → `rg_tags.csv`
3. Se cliente encontrado → `Share=1.0`, `AllocationMethod=BACKUP_SNAPSHOT_DIRECT`
4. Se não encontrado → snapshot permanece em WEIGHT_PROPORTIONAL (s05 o distribui por peso)
5. Audit: `snapshots_sem_alias_{date}.csv`

### Parte 2 — Recovery Services Vault

1. Lê `config/reference/backup_vaults.csv` (VaultName, ResourceGroup, SubscriptionId, ServiceKey)
2. Para cada vault → `GET /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.RecoveryServices/vaults/{vault}/backupProtectedItems?api-version=2023-04-01`
3. Para cada protected item, resolve cliente: `friendlyName → alias`, então `sourceResourceId.RG → rg_tags`
4. Items sem mapeamento → pool COMPARTILHADO redistribuído proporcionalmente (mesmo padrão do s03b)
5. `Share = items_cliente / mapped_count` (denominador exclui COMPARTILHADO → redistributa proporcional)
6. `AllocationMethod = BACKUP_VAULT_ITEMS` | `DriverType = BACKUP_PROTECTED_ITEMS`
7. Fallback: se ARM API falhar → log warning, vault fica em WEIGHT_PROPORTIONAL, pipeline não aborta
8. Audit: `vault_detail_{date}.csv` (VaultName, Cliente, ProtectedItems, TotalItems, CompartilhadoItems)

**Config:** `config/reference/backup_vaults.csv` — adicionar novas linhas para novos vaults sem alterar código.

**Permissões necessárias:** `Reader` nas subscriptions (já atribuído em aks-prd-newkmm identity) — suficiente para ARM list de backupProtectedItems.

**Vaults configurados em `backup_vaults.csv`:**
| VaultName | ResourceGroup | SubscriptionId | Custo/dia |
|---|---|---|---|
| kmm-prd-default-bvault-us | rg-kmm-backup | 52d4423b (KMM PRD) | ~R$75,60 |
| vault-m41c2l5l | rg-dev-kmm | 3f6d197f (KMM DEV) | ~R$1,14 |

**Status atual (2026-05-28):** operacional em produção (cq18). Validado 3 dias (2026-05-01 a 03). Retrocarga completa maio pendente.

---

## s03c — lógica de alocação SQL Elastic Pool

### Pool dedicado (tag FINOPS-CLIENTE no próprio pool)
1. Verifica `EffectiveFinopsCliente` do pool em `resource_to_service`
2. Se pool tem tag direta (≠ COMPARTILHADO, ≠ vazio) → `Share=1.0` para esse cliente, `AllocationMethod=DEDICATED_TAG`
3. **Não chama ARM API nem Azure Monitor** — pool dedicado = custo 100% do dono da tag
4. `DriverType = SQLPOOL_DEDICATED_POOL`

### Pool compartilhado (sem tag ou tag=COMPARTILHADO)
1. Para cada pool → `GET .../elasticPools/{pool}/databases` (ARM API)
2. Para cada database → CPU média via Azure Monitor (`cpu_percent`, agregação diária)
3. Mapeia database → cliente via tag `FINOPS-CLIENTE` no database
4. Databases com tag COMPARTILHADO (ou sem tag) → WeightedUsage redistribuído proporcionalmente aos clientes tagueados do mesmo pool; não geram linha em `allocation_share_final`
5. `Share = WeightedUsage_cliente / total_tagged_weighted_pool`
6. `AllocationMethod = SQLPOOL_CPU`
7. Fallback: CPU = 0 recebe peso mínimo `0.01` para não ser descartado silenciosamente

**Permissões necessárias:** `Reader` nas subscriptions (já atribuído) — suficiente para ARM list e Monitor metrics.

**Status atual (2026-06-09):** Fix cq19 — pools com tag FINOPS-CLIENTE recebem `DEDICATED_TAG` + `Share=1.0` sem coleta de métricas. Pool `sqldb-elastic-pool-vcore-prd-maroni-cw` (tag=MARONI) agora aparece como custo dedicado em vez de SQLPOOL_CPU no Power BI. Retrocarga maio necessária para refletir no consolidado.

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
# Via script genérico (aceita --start/--end ou env vars RETRO_START/RETRO_END)
python sh/retro_fix.py --start 2026-05-01 --end 2026-05-07

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
for f in cluster_config.csv clientes_alias.csv aks_system_namespaces.csv backup_vaults.csv; do
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
