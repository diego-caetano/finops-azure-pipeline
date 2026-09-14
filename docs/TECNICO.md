# Documentação Técnica

## Infraestrutura

### Storage Accounts

| Storage | Papel | Container | Permissão necessária |
|---|---|---|---|
| `azurecostexportfocus10` | Entrada — FOCUS Report | `azurecostexportfocus10` | `Storage Blob Data Reader` |
| `stpslkmmfinopseusprd` | Saída — pipeline | `finops-v2` | `Storage Blob Data Contributor` |
| `stpslkmmfinopseusprd` | Pipeline PS legado | `finops` | **Não tocar** — pipeline PS continua ativo |

### Subscriptions processadas

| Nome | GUID | Papel |
|---|---|---|
| Azure EA - KMM - PRD | `52d4423b-7ed9-4673-b8e2-fa21cdb83176` | Principal |
| Azure EA - KMM - DEV-TEST | `3f6d197f-f70b-4c2c-b981-8bb575d47a7a` | DEV/HML |
| Azure EA - NSTECH - PRD | `0535039d-8b8e-4ac1-9fce-9f727e8ba2b1` | Apenas leitura do FOCUS |

### Clusters AKS — pipeline

| Cluster | RG | Tipo | OpenCost URL | Status |
|---|---|---|---|---|
| aks-prd-newkmm | RG-US-PRD-KMM | shared | http://10.152.26.15:9003 | ✅ cluster central |
| aks-prd-br-newkmm | RG-PRD-KMM | shared | http://10.151.24.6:9003 | ✅ |
| aks-hml-newkmm | — | shared | http://10.152.12.117:9003 | ✅ |
| aks-mg-kmm-firewall-eus-prd | RG-US-PRD-KMM | shared | http://10.152.51.4:9003 | ✅ |
| aks-mg-kmm-firewall-eus-hml | RG-DEV-KMM | shared | http://10.153.6.6:9003 | ✅ |
| aks-mg-kmm-dados-eus-prd | — | shared | — | ❌ OpenCost pendente |
| aks-mg-nst-core-eus-prd | rg-mg-nst-core-eus-prd (sub MG Services PRD, `80d65916`) | shared | — | ❌ OpenCost pendente — cluster novo (migração KMM→MG), ver bloqueio abaixo |
| aks-mg-nst-core-eus-hml | rg-mg-nst-core-eus-hml (sub MG Services HML, `640f2402`) | shared | — | ❌ OpenCost pendente — cluster novo (migração KMM→MG), ver bloqueio abaixo |
| aks-mg-kmm-nocst-eus-prd-01 | — | dedicated | NORCOAST | N/A — clusters dedicados entram como DEDICATED_TAG |
| aks-mg-kmm-nocst-eus-hml-01 | — | dedicated | NORCOAST | N/A |
| aks-mg-kmm-nocst-eus-qa-01 | — | dedicated | NORCOAST | N/A |

> `aks-prd-emissor` e `aks-mg-kmm-win-eus-hml` foram removidos do tenant (confirmado via Resource Graph em 2026-09-11) — não existem mais.

> **Cluster central:** `aks-prd-newkmm` é o único que roda o pipeline principal (`finops-pipeline`) e o sync de tags (`finops-sync-rg-tags`). Os demais clusters executam apenas o job de OpenCost (`finops-aks`).

> **Migração KMM→MG (2026-09-14):** os recursos das subscriptions KMM estão sendo migrados para as
> subscriptions MG Services; as duas pontas coexistem até a migração terminar. `aks-mg-nst-core-eus-prd`
> e `aks-mg-nst-core-eus-hml` já têm carga real de cliente (namespaces ativos há semanas/meses) mas
> não tinham CronJob nem Managed Identity, e não estavam em `cluster_config.csv`/`opencost_clusters.csv`
> — os dois RGs (e o `MC_...` de nós, gerado automaticamente) estão tagueados
> `FINOPS-CLIENTE=COMPARTILHADO`, mas isso **não é um bloqueio**: `s02_classify.py` já ignora essa tag
> de propósito para RGs `MC_*` que batem com um cluster cadastrado em `cluster_config.csv` (variável
> `rule4_wins`), gerando `ServiceKey=AKS:<cluster>` normalmente. `cluster_config.csv` já foi atualizado
> com as duas linhas — falta só instalar o OpenCost e aplicar CronJob/Managed Identity. Runbook
> completo: `docs/MANUTENCAO.md#adicionar-um-novo-cluster-aks-compartilhado`. Detalhe completo da
> investigação: `CLAUDE.md` → Pendências abertas → "Migração KMM → MG Services".

---

## Estrutura de pastas no Blob (finops-v2)

```
finops-v2/
├── config/reference/
│   ├── cluster_config.csv           ← clusters AKS (tipo, URL, cliente dedicado)
│   ├── clientes_alias.csv           ← namespace/nome → cliente (89+ entradas)
│   ├── aks_system_namespaces.csv    ← namespaces de sistema a redistribuir
│   ├── overrides_rg_auto.csv        ← padrões MC_* (gerado automático pelo s02)
│   ├── rg_tags.csv                  ← tags dos RGs (gerado diário pelo s00)
│   └── backup_vaults.csv            ← vaults para s03d
│
├── silver/
│   ├── fact_cost/dt=yyyy-MM-dd/
│   ├── resource_to_service/dt=yyyy-MM-dd/
│   └── client_weights/dt=yyyy-MM-dd/
│
├── gold/
│   ├── dedicated_allocation_share/dt=yyyy-MM-dd/
│   ├── aks_allocation_share_opencost/dt=yyyy-MM-dd/
│   │   ├── aks_allocation_share_<date>.csv
│   │   └── namespaces_sem_alias_<date>.csv        ← auditoria
│   ├── sql_pool_allocation_share/dt=yyyy-MM-dd/
│   │   ├── sqlpool_allocation_share_all_<date>.csv
│   │   ├── sqlpool_detail_all_<date>.csv
│   │   └── databases_sem_tag_<date>.csv           ← auditoria
│   ├── backup_allocation_share/dt=yyyy-MM-dd/
│   │   ├── backup_share_<date>.csv
│   │   ├── vault_detail_<date>.csv                ← auditoria
│   │   └── snapshots_sem_alias_<date>.csv         ← auditoria
│   ├── allocation_share_final/dt=yyyy-MM-dd/
│   ├── fact_allocated_cost/dt=yyyy-MM-dd/
│   │   ├── fact_allocated_cost_<date>.parquet
│   │   ├── final_cost_by_client_<date>.parquet
│   │   ├── final_cost_detailed_<date>.parquet
│   │   ├── reconciliation_<date>.csv
│   │   └── unallocated_cost_<date>.csv
│   ├── fact_allocated_cost_consolidated/
│   │   ├── fact_allocated_cost_YYYY-MM.parquet  ← um arquivo por mês (Power BI)
│   │   └── manifest.csv                         ← todas as datas/meses
│   └── quality_alerts/dt=yyyy-MM-dd/            ← saída do s08
│       ├── namespaces_sem_alias_summary_<date>.csv
│       └── unallocated_actionable_<date>.csv
```

> **Consolidado mensal:** o arquivo `fact_allocated_cost_YYYY-MM.parquet` é atualizado diariamente durante o mês (D-3 + retro_fix semanal). Considera-se fechado definitivamente após o retro_fix da primeira segunda-feira do mês seguinte (ex: maio fecha após 07/jun). Usar `DASHBOARD_MONTH=YYYY-MM` para selecionar o mês no Power BI.

---

## Schedules

```
04:00 UTC           → s00 Sync RG Tags     (aks-prd-newkmm)
05:00 UTC           → s03b AKS OpenCost    (um CronJob por cluster compartilhado)
06:00 UTC           → Pipeline principal   (aks-prd-newkmm)
                      s01→s02→s03a→s03c→s03d→s04→s05→s06→s07→s08
                      Processa D-3 BRT
07:00 UTC (2ª-feira)→ finops-retro-fix     (aks-prd-newkmm)
                      Reprocessa últimos 7 dias com FOCUS atualizado
```

---

## Variáveis de ambiente

```bash
# Storage de entrada (FOCUS Report — somente leitura)
FOCUS_STORAGE_ACCOUNT=azurecostexportfocus10
FOCUS_SUBSCRIPTION_ID=0535039d-8b8e-4ac1-9fce-9f727e8ba2b1
FOCUS_CONTAINER=mca
FOCUS_PREFIX=KMM/MCA-KMM-focus-cost

# Subscriptions incluídas no pipeline
BILLING_SUBSCRIPTION_IDS=52d4423b-7ed9-4673-b8e2-fa21cdb83176,3f6d197f-f70b-4c2c-b981-8bb575d47a7a

# Storage de saída
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
BACKUP_OUT_PREFIX=gold/backup_allocation_share
CLIENT_WEIGHTS_PREFIX=silver/client_weights
ALLOCATION_SHARE_FINAL_PREFIX=gold/allocation_share_final
FACT_ALLOCATED_COST_PREFIX=gold/fact_allocated_cost
PBI_CONSOLIDATED_PREFIX=gold/fact_allocated_cost_consolidated   # prefix mensal
PBI_MANIFEST_BLOB=gold/fact_allocated_cost_consolidated/manifest.csv
QUALITY_ALERTS_PREFIX=gold/quality_alerts

# OpenCost (usado apenas no job AKS)
OPENCOST_ENDPOINT=http://opencost.opencost.svc:9003
OPENCOST_TIMEOUT_SEC=60
OPENCOST_RETRY_COUNT=2
OPENCOST_RETRY_DELAY_SEC=5

# Data de processamento (vazio = D-3 automático)
PIPELINE_DATE=
LOG_LEVEL=INFO
```

---

## Scripts — função de cada step

### s00 — Sync RG Tags
Varre todas as subscriptions de billing via ARM API e coleta a tag `FINOPS-CLIENTE` de cada Resource Group. Salva `config/reference/rg_tags.csv` no Blob. Base para herança de tag em s02.

**Schedule:** 04:00 UTC | **Cluster:** aks-prd-newkmm

---

### s01 — Ingest FOCUS
Lê o parquet mais recente do FOCUS Report para a data alvo (D-3). Filtra subscriptions `BILLING_SUBSCRIPTION_IDS`. Usa `EffectiveCost` (amortiza RIs). Remove `EffectiveCost=0` (purchase lines de RI). Salva `silver/fact_cost`.

> **Detalhe importante:** o FOCUS tem múltiplas linhas por ResourceId (uma por meter/billing line). Todas são preservadas — o s06 consolida os meters antes do fan-out.

---

### s02 — Classify
Aplica a hierarquia de 6 regras a cada recurso do `fact_cost`. Produz `silver/resource_to_service` com `ServiceKey`, `ServiceType`, `Rule`, `Confidence`.

Overrides pós-hierarquia:
- `microsoft.sql/servers/elasticpools` → `ServiceKey=SQLPOOL:{pool_name}`, `ServiceType=sql_pool`
- `microsoft.recoveryservices/vaults` → `ServiceKey=VAULT:{name}`, `ServiceType=backup_vault`
- `microsoft.compute/snapshots` → `ServiceType=snapshot`

---

### s03a — Dedicated
Recursos com `Rule ∈ {TAG_RESOURCE, TAG_RG_INHERITED}` recebem `Share=1.0` para o cliente identificado. `AllocationMethod=DEDICATED_TAG`.

---

### s03b — AKS OpenCost
**Schedule:** 05:00 UTC — CronJob separado, um por cluster compartilhado.

Consulta OpenCost e obtém custo por namespace. Mapeia namespace → cliente via `clientes_alias.csv`. Namespaces `DEFAULT` e `COMPARTILHADO` são redistribuídos proporcionalmente entre os clientes mapeados (não geram linha de cliente). Gera `namespaces_sem_alias_{date}.csv` para auditoria.

---

### s03c — SQL Pool
Para cada SQL Elastic Pool, consulta databases via ARM API e CPU via Azure Monitor. Calcula share por CPU tagueada. Databases sem tag (`COMPARTILHADO`) são redistribuídos proporcionalmente entre os tagueados do mesmo pool. Gera `databases_sem_tag_{date}.csv` para auditoria.

---

### s03d — Backup e Snapshots
**Snapshots:** extrai nome da VM → busca alias → `BACKUP_SNAPSHOT_DIRECT` (Share=1.0) ou fallback WEIGHT_PROPORTIONAL.

**Vaults:** consulta `backupProtectedItems` via ARM API → mapeia item protegido → cliente → `BACKUP_VAULT_ITEMS`. Share = items_cliente / total_items_mapeados. Fallback: mantém em WEIGHT_PROPORTIONAL se API falhar (não aborta pipeline).

**Vaults configurados:**
| VaultName | ResourceGroup | SubscriptionId | Custo/dia |
|---|---|---|---|
| kmm-prd-default-bvault-us | rg-kmm-backup | 52d4423b (KMM PRD) | ~R$75,60 |
| vault-m41c2l5l | rg-dev-kmm | 3f6d197f (KMM DEV) | ~R$1,14 |

---

### s04 — Client Weights
Calcula peso de cada cliente baseado no custo dedicado já alocado (s03a + s03b + s03c + s03d). Salva `silver/client_weights`. Usado em s05 para distribuir o WEIGHT_PROPORTIONAL.

---

### s05 — Consolidate
Une todos os shares (s03a + s03b + s03c + s03d) em `allocation_share_final`. Valida soma ≤ 1,0 por ServiceKey. Distribui o residual (recursos sem dono identificado) proporcionalmente pelo peso dos clientes. Garante 100% alocado.

---

### s06 — Fact Allocated Cost
1. Lê `fact_cost` e `allocation_share_final`
2. **Agrega fact_cost por `(Date, ResourceId, ServiceKey)` somando Cost** — consolida múltiplos meters em 1 linha por recurso antes do join
3. Fan-out por cliente: `AllocatedCost = Cost × Share`
4. Gera 5 outputs em `gold/fact_allocated_cost/dt={date}/`:
   - `fact_allocated_cost_{date}.parquet` — linha por (recurso, cliente)
   - `final_cost_by_client_{date}.parquet` — visão executiva por cliente
   - `final_cost_detailed_{date}.parquet` — drill-down por ResourceId
   - `reconciliation_{date}.csv` — validação custo bruto vs alocado
   - `unallocated_cost_{date}.csv` — recursos não alocados (deve ser vazio)

---

### s07 — Consolidate PBI
Append idempotente do dia no consolidado mensal `fact_allocated_cost_YYYY-MM.parquet`. Atualiza `manifest.csv` com status, linhas e totais do dia. Valida consistência (diff < R$ 1,00 entre TotalAllocated e TotalCost).

> O consolidado é **mensal** — um arquivo por mês. O Power BI conecta ao prefix `gold/fact_allocated_cost_consolidated/` e seleciona o mês via `DASHBOARD_MONTH`.

---

### s08 — Quality Check
Verifica qualidade dos dados após a alocação:
- Agrega `namespaces_sem_alias_*` de todos os clusters — namespaces sem mapeamento de cliente
- Filtra `unallocated_cost_{date}.csv` por reasons acionáveis (`NO_SERVICEKEY`, `HAS_SERVICEKEY_NO_SHARE`)
- Gera alertas em `gold/quality_alerts/dt={date}/`
- Log `⚠` para cada anomalia — não aborta o pipeline

---

## Operação diária

```bash
# Monitorar jobs do dia
kubectl get jobs -n finops --sort-by=.metadata.creationTimestamp

# Ver logs do pipeline principal
kubectl logs -f job/<nome-do-job> -n finops

# Teste manual para uma data específica
kubectl get cronjob finops-pipeline -n finops -o json | python3 -c "
import sys, json
d = json.load(sys.stdin)
spec = d['spec']['jobTemplate']['spec']
for c in spec['template']['spec']['containers']:
    if c['name'] == 'finops-pipeline':
        c.setdefault('env', []).append({'name': 'PIPELINE_DATE', 'value': '2026-05-01'})
print(json.dumps({'apiVersion':'batch/v1','kind':'Job',
    'metadata':{'name':'finops-manual-test','namespace':'finops'},
    'spec':spec}))
" | kubectl apply -f -

# Retrocarga manual
python sh/retro_fix.py --start 2026-05-01 --end 2026-05-07

# Reconstruir consolidado após retrocarga
kubectl delete job finops-fix-consolidated -n finops --ignore-not-found=true
kubectl apply -f k8s/job-fix-consolidated.yaml
kubectl wait --for=condition=complete --timeout=600s job/finops-fix-consolidated -n finops
kubectl logs job/finops-fix-consolidated -n finops | grep -E "s07|concluida|ERROR"
```

---

## Identidades (Workload Identity)

| Cluster | Identity | CLIENT_ID | Permissões |
|---|---|---|---|
| aks-prd-newkmm | id-mg-kmm-finops-aks-prd-newkmm-eus-prd | `40995235-e00b-417e-83a6-4ca383f9d169` | Contributor finops-v2 + Reader subs KMM + Reader FOCUS |
| aks-prd-br-newkmm | id-mg-kmm-finops-aks-prd-br-newkmm-eus-prd | `e67c4ddc-1e3f-451b-a9bd-b41296bf2834` | Reader FOCUS + Contributor finops-v2 |
| aks-hml-newkmm | id-mg-kmm-finops-aks-newkmm-eus-hml | `5eed8b89-7707-44cf-85e3-a6f86866a25f` | Reader FOCUS + Contributor finops-v2 |
| aks-mg-kmm-firewall-eus-prd | id-mg-kmm-finops-aks-firewall-eus-prd | `23fe2b59-90a1-450d-8531-de76eba5340a` | Reader FOCUS + Contributor finops-v2 |
| aks-mg-kmm-firewall-eus-hml | id-mg-kmm-finops-aks-firewall-eus-hml | `bffedfaa-5c53-4409-988c-c6884579cebc` | Reader FOCUS + Contributor finops-v2 |

---

## Build e deploy

```bash
# Build e push da imagem
az acr build --registry registrykmm --image finops-pipeline:latest .

# Verificar imagem publicada
az acr repository show-tags --name registrykmm --repository finops-pipeline

# Upload de arquivos de referência
for f in cluster_config.csv clientes_alias.csv aks_system_namespaces.csv backup_vaults.csv; do
  az storage blob upload \
    --account-name stpslkmmfinopseusprd \
    --container-name finops-v2 \
    --name "config/reference/$f" \
    --file "config/reference/$f" \
    --auth-mode login
done
```

---

## Convenções de código

| Convenção | Valor |
|---|---|
| Separador CSV | `;` |
| Encoding | UTF-8 |
| Decimais | 6 casas, ponto como separador |
| Timezone | BRT = UTC-3 (sem DST) |
| Particionamento Blob | `dt=yyyy-MM-dd` |
| Autenticação | Sempre `DefaultAzureCredential` — nunca hardcode |
| Logs | Prefixos emoji: 📅 ⬇️ ⬆️ ✅ ⚠️ ❌ |
| Retry | 3 tentativas com backoff exponencial em todo I/O |

---

## Validação do pipeline (checklist pós-deploy)

```python
import pandas as pd

# Baixar o consolidado do mês atual
df = pd.read_parquet('gold/fact_allocated_cost_consolidated/fact_allocated_cost_2026-05.parquet')

# 1. Zero problemas de qualidade
assert df['AllocationMethod'].isna().sum() == 0, "NaN AllocationMethod"
assert (df['Cliente'] == 'DEFAULT').sum() == 0, "Cliente DEFAULT"
assert df[df['Cliente']=='COMPARTILHADO']['AllocatedCost'].sum() == 0, "COMPARTILHADO residual"

# 2. Soma fecha
total = df['AllocatedCost'].sum()
soma = df.groupby('AllocationMethod')['AllocatedCost'].sum().sum()
assert abs(total - soma) < 1.0, f"Soma não fecha: {total} vs {soma}"

# 3. Vault fora do WEIGHT_PROPORTIONAL
vault_wp = df[(df['ResourceType']=='Recovery Services vault') &
              (df['AllocationMethod']=='WEIGHT_PROPORTIONAL')]
assert len(vault_wp) == 0, "Vault ainda em WEIGHT_PROPORTIONAL"

print(f"✅ Total: R$ {total:,.2f} | Linhas: {len(df):,} | Datas: {df['Date'].nunique()}")
print(df.groupby('AllocationMethod')['AllocatedCost'].sum().sort_values(ascending=False).to_string())
```
