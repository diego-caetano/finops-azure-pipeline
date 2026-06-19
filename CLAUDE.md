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
```
Permissão necessária: `Storage Blob Data Reader`

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
    cluster_config.csv | clientes_alias.csv | aks_system_namespaces.csv
    overrides_rg_auto.csv | rg_tags.csv | backup_vaults.csv

  silver/fact_cost/dt=yyyy-MM-dd/
  silver/resource_to_service/dt=yyyy-MM-dd/
  silver/client_weights/dt=yyyy-MM-dd/

  gold/aks_allocation_share_opencost/dt=yyyy-MM-dd/
  gold/sql_pool_allocation_share/dt=yyyy-MM-dd/
  gold/backup_allocation_share/dt=yyyy-MM-dd/
  gold/allocation_share_final/dt=yyyy-MM-dd/
  gold/fact_allocated_cost/dt=yyyy-MM-dd/
  gold/fact_allocated_cost_consolidated/
    fact_allocated_cost_YYYY-MM.parquet   ← um arquivo por mês para BI
    manifest.csv                           ← uma linha por data (Status, Rows, TotalAllocated)
```

### 3. Storage do Pipeline PS atual (não tocar)
```
Account  : stpslkmmfinopseusprd
Container: finops   ← manter intacto, pipeline PS continua rodando
```

---

## Autenticação — Workload Identity

Identidades configuradas (todas operacionais):

| Cluster | Identity name | Resource Group |
|---|---|---|
| aks-prd-newkmm | id-mg-kmm-finops-aks-prd-newkmm-eus-prd | RG-US-PRD-KMM |
| aks-prd-br-newkmm | id-mg-kmm-finops-aks-prd-br-newkmm-eus-prd | RG-PRD-KMM |
| aks-hml-newkmm | id-mg-kmm-finops-aks-newkmm-eus-hml | RG-US-PRD-KMM |
| aks-mg-kmm-firewall-eus-prd | id-mg-kmm-finops-aks-firewall-eus-prd | RG-US-PRD-KMM |
| aks-mg-kmm-firewall-eus-hml | id-mg-kmm-finops-aks-firewall-eus-hml | RG-DEV-KMM (sub: 3f6d197f) |

Para adicionar novo cluster: ver `docs/HISTORY.md#workload-identity-setup`.

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

Override pós-hierarquia:
- `microsoft.sql/servers/elasticpools` → `ServiceKey=SQLPOOL:{pool_name}` — processado por s03c
- `microsoft.recoveryservices/vaults` → `ServiceKey=VAULT:{ResourceName}` — processado por s03d
- `microsoft.compute/snapshots` → `ServiceType=snapshot` — processado por s03d

---

## Clusters AKS

| Cluster | Resource Group | OpenCost |
|---|---|---|
| aks-prd-newkmm | RG-US-PRD-KMM | ✅ http://10.152.26.15:9003 |
| aks-prd-br-newkmm | RG-PRD-KMM | ✅ http://10.151.24.6:9003 |
| aks-hml-newkmm | RG-US-PRD-KMM | ✅ http://10.152.12.117:9003 — OPENCOST_TIMEOUT_SEC=180 (ConfigMap local; cluster spot tem muitos nós) |
| aks-mg-kmm-firewall-eus-prd | RG-US-PRD-KMM | ✅ http://10.152.51.4:9003 |
| aks-mg-kmm-firewall-eus-hml | RG-DEV-KMM (sub: 3f6d197f — usar `--subscription 3f6d197f-f70b-4c2c-b981-8bb575d47a7a`) | ✅ http://10.153.6.6:9003 |
| aks-mg-kmm-dados-eus-prd | (confirmar) | ❌ pendente instalação |
| aks-prd-emissor | (confirmar) | ❌ pendente migração |
| aks-mg-kmm-win-eus-hml | (confirmar) | ❌ pendente instalação |
| aks-mg-kmm-nocst-eus-*-01 | (confirmar) | ❌ dedicated NORCOAST — não necessário |

---

## Steps do pipeline e schedules

```
04:00 UTC           → s00_sync_rg_tags   (varre RGs → rg_tags.csv)
05:00 UTC           → s03b_aks           (dentro de cada cluster compartilhado)
06:00 UTC           → pipeline principal (s01→s02→s03a→s03c→s03d→s04→s05→s06→s07→s08) — D-3 BRT
07:00 UTC (2ª-feira)→ finops-retro-fix   (reprocessa últimos 7 dias com FOCUS atualizado)
```

| Step | Arquivo | Descrição |
|---|---|---|
| s00 | `finops/steps/s00_sync_rg_tags.py` | Varre RGs, salva rg_tags.csv |
| s01 | `finops/steps/s01_ingest_focus.py` | Lê FOCUS parquet → fact_cost; usa `EffectiveCost` (não BilledCost) |
| s02 | `finops/steps/s02_classify.py` | Classifica recursos → resource_to_service |
| s03a | `finops/steps/s03a_dedicated.py` | Recursos dedicados, share=1.0 |
| s03b | `finops/steps/s03b_aks.py` | AKS via OpenCost; DEFAULT/COMPARTILHADO redistribuídos; fallback para shares históricos (`AKS_OPENCOST_CACHED`) se OpenCost indisponível |
| s03c | `finops/steps/s03c_sql_pool.py` | SQL Pool: pool dedicado → DEDICATED_TAG; compartilhado → CPU(70%)+DiskGB(30%) |
| s03d | `finops/steps/s03d_backup.py` | Snapshots → alias direto; Vaults → protected items ARM API |
| s04 | `finops/steps/s04_client_weights.py` | Peso por cliente |
| s05 | `finops/steps/s05_consolidate.py` | Consolida shares, valida soma=1.0 |
| s06 | `finops/steps/s06_fact_allocated_cost.py` | AllocatedCost = BilledCost × Share; agrega fact_cost por ResourceId ANTES do join |
| s07 | `finops/steps/s07_consolidate_pbi.py` | Append diário idempotente + manifest.csv |
| s08 | `finops/steps/s08_quality_check.py` | Alertas de qualidade: (1) namespaces sem alias, (2) recursos não alocados, (3) clusters AKS usando shares cacheados (`AKS_OPENCOST_CACHED`) |

**FOCUS_LAG_DAYS = 3** (em `finops/utils/dates.py`): pipeline processa D-3; retro_fix semanal cobre lags maiores.

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
BACKUP_OUT_PREFIX=gold/backup_allocation_share
QUALITY_ALERTS_PREFIX=gold/quality_alerts

# OpenCost (usado apenas no job AKS)
OPENCOST_ENDPOINT=http://opencost.opencost.svc:9003
OPENCOST_TIMEOUT_SEC=60   # padrão; aks-hml-newkmm tem 180 no ConfigMap local (cluster com muitos nodes spot)
OPENCOST_RETRY_COUNT=2
OPENCOST_RETRY_DELAY_SEC=5

# Pipeline
PIPELINE_DATE=        # vazio = automático D-3 BRT; para retrocarga manual: yyyy-MM-dd
LOG_LEVEL=INFO

# SQL Pool weights
SQL_CPU_WEIGHT=0.70
SQL_DISK_WEIGHT=0.30
```

---

## Schemas dos arquivos

```
fact_cost:
  Date;ResourceId;ResourceName;ResourceGroupName;ResourceType;SubscriptionId;ServiceName;Cost

rg_tags.csv:
  ResourceGroupName;FinopsCliente;SubscriptionId;UpdatedAt

resource_to_service:
  Date;ResourceId;ResourceType;ResourceName;ResourceGroupName;
  ServiceKey;ServiceType;Rule;Confidence;EffectiveFinopsCliente;FinopsClienteSource;Notes

allocation_share_final:
  Date;ServiceKey;Cliente;Share;DriverType;AllocationMethod

fact_allocated_cost (diário e consolidado mensal — mesmo schema):
  Date;ResourceId;ResourceName;ResourceGroupName;ResourceType;SubscriptionId;
  ServiceName;ServiceKey;Cliente;Cost;Share;AllocatedCost;AllocationMethod;DriverType

  Granularidade: 1 linha = (Date, ResourceId, ServiceKey, Cliente)
  ResourceName = nome do recurso Azure de billing em todos os métodos:
    DEDICATED_TAG          → recurso dedicado (1 cliente)
    SQLPOOL_CPU            → nome do pool (não do database)
    AKS_OPENCOST           → nome do node/VM de billing (namespace está no ServiceKey)
    AKS_OPENCOST_CACHED    → mesmo que AKS_OPENCOST, mas OpenCost estava indisponível; shares do dia anterior reutilizados
    WEIGHT_PROPORTIONAL    → recurso compartilhado (aparece N vezes, 1 por cliente)

manifest.csv:
  Date;Status;Rows;TotalAllocated;TotalCost;ProcessedAt;PipelineRun
```

**Atenção — Power BI:** usar sempre `SUM(AllocatedCost)`, nunca `SUM(Cost)`. O campo `Cost` é repetido por cliente no fan-out do s06 e fica inflado.

**Mutabilidade:** arquivos diários são sobrescritos pelo pipeline D-3 e pelo retro_fix semanal. Mês considera-se fechado após D+3 do último dia (ex: maio fecha em 03/06).

---

## Estado atual — tudo operacional ✅

### Código Python — 100% implementado
- [x] `finops/utils/dates.py`, `finops/utils/logging.py`, `finops/storage.py`
- [x] `finops/steps/s00_sync_rg_tags.py`
- [x] `finops/steps/s01_ingest_focus.py` — `EffectiveCost` (não BilledCost); filtra `EffectiveCost>0`; normaliza ARM prefix
- [x] `finops/steps/s02_classify.py` — 6 regras, MC_ pattern, elastic pools/vaults/snapshots override
- [x] `finops/steps/s03a_dedicated.py`
- [x] `finops/steps/s03b_aks.py` — DEFAULT/COMPARTILHADO redistribuídos; gera `namespaces_sem_alias_{date}.csv`; fallback `_load_last_known_shares` quando OpenCost indisponível (→ `AKS_OPENCOST_CACHED`, nunca WEIGHT_PROPORTIONAL)
- [x] `finops/steps/s03c_sql_pool.py` — pool dedicado → DEDICATED_TAG sem API; compartilhado → CPU(70%)+DiskGB(30%); gera `databases_sem_tag_{date}.csv`
- [x] `finops/steps/s03d_backup.py` — snapshot→alias; vault→ARM backupProtectedItems; fallback WEIGHT_PROPORTIONAL; gera audit CSVs
- [x] `finops/steps/s04_client_weights.py`
- [x] `finops/steps/s05_consolidate.py` — `_read_csv_partition` com include/exclude; colapsa AKS por (Date,ServiceKey,Cliente); filtra DEFAULT histórico
- [x] `finops/steps/s06_fact_allocated_cost.py` — agrega fact_cost por ResourceId ANTES do join (meter fan-out guard); 5 outputs
- [x] `finops/steps/s07_consolidate_pbi.py` — append idempotente; TotalCost via reconciliation; schema alignment
- [x] `finops/steps/s08_quality_check.py` — 3 checks: (1) NAMESPACE_SEM_ALIAS com lista compacta numa linha (campo `Ocorrências:` no e-mail), (2) RECURSO_NAO_ALOCADO, (3) AKS_OPENCOST_CACHED
- [x] `jobs/run_pipeline.py`, `jobs/run_aks.py`, `jobs/run_sync_rg_tags.py`
- [x] `sh/retro_fix.py` — aceita `--start`/`--end` ou `RETRO_START`/`RETRO_END`; consolidação PBI em lote único ao final
- [x] `sh/build_consolidated.py`

### Kubernetes
- [x] `k8s/cronjob-pipeline.yaml`, `k8s/cronjob-aks.yaml`, `k8s/cronjob-sync-rg-tags.yaml`
- [x] `k8s/cronjob-retro-fix.yaml` — toda 2ª-feira 07:00 UTC
- [x] `k8s/job-fix-consolidated.yaml`, `k8s/job-retro.yaml`

### Infraestrutura
- [x] ACR: `registrykmm.azurecr.io/finops-pipeline:latest` — build cq1h (2026-06-19)
- [x] 5 clusters AKS com OpenCost e CronJobs aplicados
- [x] Consolidado maio: `fact_allocated_cost_2026-05.parquet` (905.865 linhas, 31 datas) ✅

---

## Pendências abertas

### Clusters sem OpenCost (bloquear — instalar OpenCost primeiro)
- [ ] aks-mg-kmm-dados-eus-prd
- [ ] aks-prd-emissor
- [ ] aks-mg-kmm-win-eus-hml

### Power BI
- [ ] Reconectar `.pbix` via folder connector para `fact_allocated_cost_YYYY-MM.parquet` — ver `docs/POWER_BI.md`

---

## s03b — resiliência quando OpenCost está indisponível

Quando `_fetch_allocations` falha após todas as retentativas, o step **não usa WEIGHT_PROPORTIONAL** (injusto: cobraria clientes que não consomem aquele cluster). Em vez disso:

1. Chama `_load_last_known_shares(store, prefix, cluster_name, current_date)`
2. Lista `gold/aks_allocation_share_opencost/` e busca o arquivo mais recente de `{cluster_name}.csv` excluindo a data atual
3. Reutiliza os shares com `AllocationMethod=AKS_OPENCOST_CACHED` e `DriverType=OPENCOST_NAMESPACE_CACHED`
4. Grava o arquivo do dia atual no blob — o pipeline principal processa normalmente
5. Se não existir histórico, loga `❌` e não grava nada — o cluster fica sem alocação AKS para o dia

O s08 detecta arquivos com `AKS_OPENCOST_CACHED` e dispara alerta:
```
⚠ AKS_OPENCOST_CACHED: 1 cluster(s) usando shares históricos | Clusters: aks-hml-newkmm | Ação: verificar pods OpenCost
```

**aks-hml-newkmm — timeout especial:** cluster spot com ~30+ nós; OpenCost leva ~167s para computar alocações históricas.
`OPENCOST_TIMEOUT_SEC=180` aplicado diretamente no ConfigMap `finops-aks-config` no cluster (não alterar o cronjob-aks.yaml padrão).

```bash
# Verificar/replicar o override de timeout em aks-hml-newkmm:
kubectl config use-context aks-hml-newkmm
kubectl get configmap finops-aks-config -n finops -o jsonpath='{.data.OPENCOST_TIMEOUT_SEC}'
# Deve retornar: 180
```

---

## s03c — lógica de alocação SQL Elastic Pool

### Pool dedicado (tag FINOPS-CLIENTE no próprio pool)
1. Verifica `EffectiveFinopsCliente` do pool em `resource_to_service`
2. Se dedicado (≠ COMPARTILHADO, ≠ vazio) → `Share=1.0`, `AllocationMethod=DEDICATED_TAG`, `DriverType=SQLPOOL_DEDICATED_POOL`
3. Não chama ARM API nem Azure Monitor

### Pool compartilhado
1. Lista databases via ARM API
2. CPU média via Azure Monitor (`cpu_percent`) — peso 70%
3. Storage em GB absolutos via Azure Monitor (`storage` em bytes → GB) — peso 30%
4. Databases COMPARTILHADO → WeightedUsage redistribuído aos tagueados; `Share=0` no detalhe
5. `Share = WeightedUsage / total_tagged_weighted` | `AllocationMethod=SQLPOOL_CPU`
6. Fallback: CPU=0 recebe peso mínimo `0.01`

**Adicionar novo vault:** editar `config/reference/backup_vaults.csv` — sem alterar código.

---

## s03d — lógica de alocação Azure Backup e Snapshots

### Snapshots
1. Exclui snapshots já cobertos por s03a (TAG_RESOURCE/TAG_RG_INHERITED)
2. Resolve cliente: segmento de VM → `clientes_alias.csv` → fallback nome completo → fallback RG tag
3. Mapeado → `Share=1.0`, `AllocationMethod=BACKUP_SNAPSHOT_DIRECT`
4. Não mapeado → fica em WEIGHT_PROPORTIONAL (s05 distribui por peso)

### Recovery Services Vaults
1. Lê `config/reference/backup_vaults.csv`
2. ARM API `backupProtectedItems` por vault
3. Resolve cliente por item: `friendlyName → alias` ou `sourceResourceId.RG → rg_tags`
4. Itens COMPARTILHADO redistribuídos proporcionalmente aos mapeados
5. `AllocationMethod=BACKUP_VAULT_ITEMS` | fallback WEIGHT_PROPORTIONAL se ARM falhar

---

## Cluster central — aks-prd-newkmm

Roda os jobs centralizados:
- `finops-sync-rg-tags` (04:00 UTC) — s00
- `finops-pipeline` (06:00 UTC) — pipeline principal

Identity `id-mg-kmm-finops-aks-prd-newkmm-eus-prd` (CLIENT_ID: `40995235-e00b-417e-83a6-4ca383f9d169`):
- `Storage Blob Data Contributor` em `finops-v2` ✅
- `Reader` em `52d4423b` e `3f6d197f` ✅
- `Storage Blob Data Reader` em `azurecostexportfocus10` ✅

---

## Comandos úteis

```bash
# Build da imagem
$env:PYTHONIOENCODING="utf-8"
az acr build --registry registrykmm --image finops-pipeline:latest .

# Conectar no cluster e aplicar manifests
az aks get-credentials --resource-group <RG> --name <CLUSTER>
kubectl create namespace finops --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/cronjob-aks.yaml

# Teste manual — pipeline principal
az aks get-credentials --resource-group RG-US-PRD-KMM --name aks-prd-newkmm --overwrite-existing
kubectl create job --from=cronjob/finops-pipeline finops-pipeline-test-$(date +%s) -n finops
kubectl logs -f job/<nome-gerado> -n finops

# Retrocarga corretiva
python sh/retro_fix.py --start 2026-05-01 --end 2026-05-07

# Reconstruir consolidado Power BI após retrocarga
kubectl delete job finops-fix-consolidated -n finops --ignore-not-found=true
kubectl apply -f k8s/job-fix-consolidated.yaml
kubectl wait --for=condition=complete --timeout=600s job/finops-fix-consolidated -n finops

# Enviar config/reference para o Blob
for f in cluster_config.csv clientes_alias.csv aks_system_namespaces.csv backup_vaults.csv; do
  az storage blob upload \
    --account-name stpslkmmfinopseusprd --container-name finops-v2 \
    --name "config/reference/$f" --file "config/reference/$f" --auth-mode login
done
```
