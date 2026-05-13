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

## Três Storage Accounts — papéis distintos

### 1. Storage de ENTRADA — FOCUS Report (somente leitura)
```
Account     : azurecostexportfocus10
Subscription: 0535039d-8b8e-4ac1-9fce-9f727e8ba2b1  (Azure EA - NSTECH - PRD)
Container   : azurecostexportfocus10
Prefixo     : costexport/azure-cost-export-focus10-52548376/
Estrutura   : costexport/azure-cost-export-focus10-52548376/<yyyyMMdd-yyyyMMdd>/<arquivo>
Exemplo     : costexport/azure-cost-export-focus10-52548376/20260401-20260430/data.parquet
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
Container   : finops-v2   ← CRIAR ANTES DE RODAR O PIPELINE
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
  gold/allocation_share_final/dt=yyyy-MM-dd/
  gold/fact_allocated_cost/dt=yyyy-MM-dd/
```

### 3. Storage do Pipeline PS atual (não tocar)
```
Account  : stpslkmmfinopseusprd
Container: finops   ← manter intacto, pipeline PS continua rodando
```

---

## Como criar o container finops-v2 no portal

1. Portal Azure → `stpslkmmfinopseusprd` → **Containers** → **+ Container**
2. Name: `finops-v2`
3. Public access level: `Private`
4. **Create**

Ou via terminal:
```bash
az storage container create \
  --account-name stpslkmmfinopseusprd \
  --name finops-v2 \
  --auth-mode login
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

Identities necessárias (uma por cluster compartilhado):

| Cluster | Identity name | Resource Group | Status |
|---|---|---|---|
| aks-prd-newkmm | id-mg-kmm-finops-aks-prd-newkmm-eus-prd | RG-US-PRD-KMM | ⏳ pendente |
| aks-prd-br-newkmm | id-mg-kmm-finops-aks-prd-br-newkmm-eus-prd | RG-PRD-KMM | ⏳ pendente |
| aks-hml-newkmm | id-mg-kmm-finops-aks-newkmm-eus-hml | RG-US-PRD-KMM | ✅ configurado |
| aks-mg-kmm-firewall-eus-prd | id-mg-kmm-finops-aks-firewall-eus-prd | RG-US-PRD-KMM | ⏳ pendente |
| aks-mg-kmm-firewall-eus-hml | id-mg-kmm-finops-aks-firewall-eus-hml | RG-DEV-KMM (sub: 3f6d197f) | ⏳ pendente |

Comandos para criar e configurar cada identity:
```bash
# 1. Criar identity (repetir para cada cluster)
az identity create \
  --name finops-aks-identity-hml \
  --resource-group <RG-DO-CLUSTER> \
  --location eastus

# 2. Pegar Client ID e Object ID
CLIENT_ID=$(az identity show \
  --name finops-aks-identity-hml \
  --resource-group <RG-DO-CLUSTER> \
  --query clientId -o tsv)

OBJECT_ID=$(az identity show \
  --name finops-aks-identity-hml \
  --resource-group <RG-DO-CLUSTER> \
  --query principalId -o tsv)

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
OIDC_ISSUER=$(az aks show \
  --resource-group <RG-DO-CLUSTER> \
  --name aks-hml-newkmm \
  --query "oidcIssuerProfile.issuerUrl" -o tsv)

az identity federated-credential create \
  --name finops-aks-hml-federated \
  --identity-name finops-aks-identity-hml \
  --resource-group <RG-DO-CLUSTER> \
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
04:00 UTC → s00_sync_rg_tags    (varre RGs das subscriptions → rg_tags.csv)
05:00 UTC → s03b_aks            (dentro de cada cluster compartilhado)
06:00 UTC → pipeline principal  (s01 → s02 → s03a → s03c → s04 → s05 → s06)
```

| Step | Arquivo | Descrição |
|---|---|---|
| s00 | `finops/steps/s00_sync_rg_tags.py` | Varre RGs, salva rg_tags.csv |
| s01 | `finops/steps/s01_ingest_focus.py` | Lê FOCUS parquet → fact_cost |
| s02 | `finops/steps/s02_classify.py` | Classifica recursos → resource_to_service |
| s03a | `finops/steps/s03a_dedicated.py` | Recursos dedicados, share=1.0 |
| s03b | `finops/steps/s03b_aks.py` | AKS via OpenCost (já implementado) |
| s03c | `finops/steps/s03c_sql_pool.py` | SQL Pool via CPU+Disk |
| s04 | `finops/steps/s04_client_weights.py` | Peso por cliente |
| s05 | `finops/steps/s05_consolidate.py` | Consolida shares, valida soma=1.0 |
| s06 | `finops/steps/s06_fact_allocated_cost.py` | AllocatedCost = BilledCost × Share |

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

# OpenCost (usado apenas no job AKS)
OPENCOST_ENDPOINT=http://opencost.opencost.svc:9003
OPENCOST_TIMEOUT_SEC=60
OPENCOST_RETRY_COUNT=2
OPENCOST_RETRY_DELAY_SEC=5

# Pipeline
PIPELINE_DATE=        # vazio = automático D-1 Brasil
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

---

## O que já foi implementado

### Código Python
- [x] `finops/utils/dates.py`
- [x] `finops/utils/logging.py`
- [x] `finops/storage.py` — suporta dois storage accounts (FOCUS_* e PIPELINE_*)
- [x] `finops/steps/s00_sync_rg_tags.py`
- [x] `finops/steps/s01_ingest_focus.py`
- [x] `finops/steps/s02_classify.py`
- [x] `finops/steps/s03a_dedicated.py`
- [x] `finops/steps/s03b_aks.py` — usa variáveis FOCUS_* e PIPELINE_*
- [x] `finops/steps/s03c_sql_pool.py`
- [x] `finops/steps/s04_client_weights.py`
- [x] `finops/steps/s05_consolidate.py`
- [x] `finops/steps/s06_fact_allocated_cost.py`
- [x] `jobs/run_aks.py`
- [x] `jobs/run_sync_rg_tags.py`
- [x] `jobs/run_pipeline.py`

### Kubernetes / infra de código
- [x] `k8s/cronjob-aks.yaml` — CLIENT_ID e ACR preenchidos para `aks-hml-newkmm`
- [x] `k8s/cronjob-sync-rg-tags.yaml`
- [x] `k8s/cronjob-pipeline.yaml`
- [x] `Dockerfile`
- [x] `requirements.txt`

### Configuração / referência
- [x] `config/reference/cluster_config.csv`
- [x] `config/reference/clientes_alias.csv`
- [x] `CLAUDE.md`

### Infraestrutura provisionada
- [x] ACR definido: `registrykmm` → `registrykmm.azurecr.io/finops-pipeline:latest`
- [x] Build da imagem: `az acr build` publicou `registrykmm.azurecr.io/finops-pipeline:latest`
- [x] Container `finops-v2` criado em `stpslkmmfinopseusprd`
- [x] FOCUS Reader **não é necessário** para o job AKS — `s03b_aks.py` só acessa `finops-v2`

#### aks-hml-newkmm — ✅ 100% operacional (2026-04-30)
- [x] Identity: `id-mg-kmm-finops-aks-newkmm-eus-hml` (CLIENT_ID: `5eed8b89-7707-44cf-85e3-a6f86866a25f`)
- [x] `Storage Blob Data Contributor` em `stpslkmmfinopseusprd/finops-v2`
- [x] Federated Credential criado
- [x] AcrPull atribuído ao kubelet identity do cluster
- [x] CronJob aplicado — executa diariamente às 05:00 UTC

#### aks-prd-newkmm — ✅ 100% operacional (2026-04-30)
- [x] Identity: `id-mg-kmm-finops-aks-prd-newkmm-eus-prd` (CLIENT_ID: `40995235-e00b-417e-83a6-4ca383f9d169`)
- [x] `Storage Blob Data Contributor` em `stpslkmmfinopseusprd/finops-v2`
- [x] Federated Credential criado
- [x] AcrPull atribuído ao kubelet identity
- [x] CronJob aplicado — 3 execuções bem-sucedidas confirmadas (2026-05-02 a 2026-05-04)

#### aks-prd-br-newkmm — ✅ 100% operacional (2026-05-04)
- [x] Identity: `id-mg-kmm-finops-aks-prd-br-newkmm-eus-prd` (CLIENT_ID: `e67c4ddc-1e3f-451b-a9bd-b41296bf2834`)
- [x] `Storage Blob Data Contributor` em `stpslkmmfinopseusprd/finops-v2`
- [x] OIDC Issuer + Workload Identity habilitados via `az aks update`
- [x] Federated Credential criado
- [x] AcrPull atribuído ao kubelet identity
- [x] CronJob aplicado — teste: 5 namespaces → 1 cliente → gravado

#### aks-mg-kmm-firewall-eus-prd — ✅ 100% operacional (2026-05-04)
- Subscription: `52d4423b-7ed9-4673-b8e2-fa21cdb83176` | RG: `RG-US-PRD-KMM`
- [x] Identity: `id-mg-kmm-finops-aks-firewall-eus-prd` (CLIENT_ID: `23fe2b59-90a1-450d-8531-de76eba5340a`)
- [x] `Storage Blob Data Contributor` em `stpslkmmfinopseusprd/finops-v2`
- [x] Federated Credential criado
- [x] AcrPull atribuído ao kubelet identity
- [x] CronJob aplicado — teste: 6 namespaces → 1 cliente → gravado

#### aks-mg-kmm-firewall-eus-hml — ✅ 100% operacional (2026-05-04)
- Subscription: `3f6d197f-f70b-4c2c-b981-8bb575d47a7a` (Azure EA - KMM - DEV-TEST) | RG: `RG-DEV-KMM`
- [x] Identity: `id-mg-kmm-finops-aks-firewall-eus-hml` (CLIENT_ID: `bffedfaa-5c53-4409-988c-c6884579cebc`)
- [x] `Storage Blob Data Contributor` em `stpslkmmfinopseusprd/finops-v2`
- [x] Federated Credential criado
- [x] AcrPull atribuído ao kubelet identity
- [x] CronJob aplicado — teste: 6 namespaces → 1 cliente → gravado

## O que falta — infraestrutura (fazer no portal/terminal antes do código)

### Clusters com OpenCost — todos operacionais ✅
Os 5 clusters com OpenCost estão configurados e executando diariamente às 05:00 UTC.

### Clusters sem OpenCost (bloquear — instalar OpenCost primeiro)
- [ ] aks-mg-kmm-dados-eus-prd — instalar OpenCost, depois seguir o fluxo acima
- [ ] aks-prd-emissor — migrar para OpenCost, depois seguir o fluxo acima
- [ ] aks-mg-kmm-win-eus-hml — instalar OpenCost, depois seguir o fluxo acima

## O que falta — código Python (implementar nesta ordem)

- [x] `finops/steps/s00_sync_rg_tags.py` + `jobs/run_sync_rg_tags.py` + `k8s/cronjob-sync-rg-tags.yaml`
- [x] `finops/steps/s01_ingest_focus.py`
- [x] `finops/steps/s02_classify.py` — hierarquia 6 regras, MC_ pattern, Tags JSON
- [x] `finops/steps/s03a_dedicated.py` — Share=1.0 por recurso dedicado
- [x] `finops/steps/s03c_sql_pool.py` — SQL Elastic Pools: lista databases via ARM API, CPU média via Azure Monitor, redistribui não-mapeados proporcionalmente
- [x] `finops/steps/s04_client_weights.py` — peso por custo dedicado, fallback igualitário
- [x] `finops/steps/s05_consolidate.py` — consolida s03a+s03b+s03c, distribui restante por client_weights
- [x] `finops/steps/s06_fact_allocated_cost.py` — AllocatedCost = Cost × Share
- [x] `jobs/run_pipeline.py` — orquestra s01→s02→s03a→s03c→s04→s05→s06
- [x] `k8s/cronjob-pipeline.yaml` — 06:00 UTC, aks-prd-newkmm

#### aks-prd-newkmm — jobs centrais ✅ 100% operacional (2026-05-12)
- [x] `Reader` em `52d4423b` (KMM PRD) — atribuído via REST
- [x] `Reader` em `3f6d197f` (KMM DEV-TEST) — atribuído via REST
- [x] `Storage Blob Data Reader` em `azurecostexportfocus10` (sub `0535039d`) — atribuído
- [x] Rebuild da imagem com `azure-mgmt-resource` publicado em `registrykmm.azurecr.io/finops-pipeline:latest`
- [x] `finops-sync-rg-tags` CronJob aplicado — executa diariamente às 04:00 UTC
- [x] `finops-pipeline` CronJob aplicado — executa diariamente às 06:00 UTC
- [x] Testados manualmente com sucesso

## Análise do pipeline PS legado (2026-05-13)

Comparação com output do container `finops` (pipeline PowerShell) para 2026-05-12:

| Métrica | Valor |
|---|---|
| Total Fact Cost | R$ 8.610,60 |
| Total Alocado (PS) | R$ 4.338,41 (50,4%) |
| Total Não Alocado (PS) | R$ 4.272,20 (49,6%) |

**Razões de não-alocação no PS:**
- `HAS_SERVICEKEY_NO_SHARE` — 99,5% do não-alocado (305 recursos): o PS identificava o ServiceKey mas não tinha share definido
- `NO_MAPPING_BY_RESOURCEID_OR_RG` — 0,5% (3 recursos)

**Top causas por custo:**

| ServiceKey | Custo | Causa raiz | Solução no Python |
|---|---|---|---|
| SQL Elastic Pools (6 pools) | R$ 2.725 (63,9%) | PS sem lógica de rateio por utilização | s03c reescrito: ARM API + Monitor CPU |
| `AKS:aks-prd-newkmm` (infra MC_) | R$ 472 | PS sem share para infraestrutura | s03b share via OpenCost cobre todos os recursos do cluster |
| `AKS:aks-hml-newkmm` (infra MC_) | R$ 324 | idem | idem |
| INFRA / APPGW / RSV | R$ 324 | Sem tag, sem share | s05 distribui por client_weights |
| `AKS:aks-prd-br-newkmm` | R$ 36 | idem AKS | idem |

**Conclusão:** o pipeline Python resolve estruturalmente todos os gaps do PS. Após rebuild e validação end-to-end, o TotalAllocated deve ser significativamente maior que 50,4%.

---

## s03c — lógica de alocação SQL Elastic Pool (2026-05-13)

**Antes:** arquivo estático `sql_pool_weights.csv` (nunca criado) + tipo errado (Synapse).

**Depois (dinâmico):**
1. Filtra `Microsoft.Sql/servers/elasticPools` não-dedicados em `resource_to_service`
2. Para cada pool → `GET .../elasticPools/{pool}/databases` (ARM API)
3. Para cada database → CPU média via Azure Monitor (`cpu_percent`, agregação diária)
4. Mapeia database → cliente via tag `FINOPS-CLIENTE` no database
5. Databases sem tag → redistribuídos proporcionalmente aos clientes mapeados
6. `Share = cpu_cliente / cpu_total_pool`
7. Fallback: CPU = 0 recebe peso mínimo `0.01` para não ser descartado silenciosamente

**Permissões necessárias:** `Reader` nas subscriptions (já atribuído) — suficiente para ARM list e Monitor metrics.

---

## Próximos passos

### 1. Rebuild da imagem — obrigatório (s03c reescrito)
```bash
az acr build --registry registrykmm --image finops-pipeline:latest .
```

### 2. Validação end-to-end do pipeline Python
Rodar manualmente para uma data e comparar com o output do PS:
```bash
az aks get-credentials --resource-group RG-US-PRD-KMM --name aks-prd-newkmm --overwrite-existing
kubectl create job --from=cronjob/finops-pipeline finops-pipeline-test-$(date +%s) -n finops
kubectl logs -f job/<nome-gerado> -n finops
```
Verificar em `finops-v2`:
- `gold/fact_allocated_cost/dt=<data>/` — AllocatedCost total deve cobrir ~100% do custo
- `gold/sql_pool_allocation_share/dt=<data>/` — deve existir com shares por pool
- Comparar `gold/allocation_share_final` com `gold/reconciliation` do container `finops`

### 3. Tag FINOPS-CLIENTE nos databases SQL
Para o s03c funcionar corretamente, cada database nos elastic pools precisa ter a tag `FINOPS-CLIENTE` com o nome do cliente. Verificar quais databases estão sem tag e tagear.

### 4. Clusters sem OpenCost (bloqueados)
- `aks-mg-kmm-dados-eus-prd` — instalar OpenCost, depois aplicar CronJob
- `aks-prd-emissor` — migrar para OpenCost, depois aplicar CronJob
- `aks-mg-kmm-win-eus-hml` — instalar OpenCost, depois aplicar CronJob

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

### Criar container finops-v2
```bash
az storage container create \
  --account-name stpslkmmfinopseusprd \
  --name finops-v2 \
  --auth-mode login
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

### Teste manual do job AKS
```bash
kubectl create job \
  --from=cronjob/finops-aks-allocation \
  finops-aks-test-$(date +%s) \
  -n finops
kubectl logs -f job/<nome-gerado> -n finops
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
