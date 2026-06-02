# Modelo de Alocação — Regras e Dicionário

## Hierarquia de classificação

O pipeline tenta identificar o dono de cada recurso Azure seguindo esta ordem de prioridade. A primeira regra que se aplicar vence.

```
Prioridade  Condição                                        Resultado
──────────  ──────────────────────────────────────────────  ──────────────────────────
1 (máx)     Tag FINOPS-CLIENTE no recurso ≠ COMPARTILHADO   100% para o cliente → DEDICATED_TAG
2           Tag FINOPS-CLIENTE no RG ≠ COMPARTILHADO        100% para o cliente → DEDICATED_TAG
3           Tag FINOPS-CLIENTE no RG = COMPARTILHADO        Rateio proporcional → WEIGHT_PROPORTIONAL
4           RG no padrão MC_* (infra gerenciada AKS)        Rateio por OpenCost → AKS_OPENCOST
5           ResourceType = SQL Elastic Pool                  Rateio por CPU → SQLPOOL_CPU
6           ResourceType = Recovery Services Vault          Rateio por itens protegidos → BACKUP_VAULT_ITEMS
7           ResourceType = Snapshot                         Direto pelo nome da VM → BACKUP_SNAPSHOT_DIRECT
8 (mín)     Sem classificação                               Rateio por peso → WEIGHT_PROPORTIONAL
```

---

## Métodos de alocação

### DEDICATED_TAG

**O que é:** recurso tem tag `FINOPS-CLIENTE=ClienteX` no próprio recurso ou no seu Resource Group.

**Como funciona:** `Share = 1,0` para o cliente identificado. 100% do custo vai para ele.

**Exemplos:**
- VM `vm-norcoast-prd-01` com tag `FINOPS-CLIENTE=NORCOAST` → 100% NORCOAST
- RG `rg-maroni-prd` com tag `FINOPS-CLIENTE=MARONI` → todos os recursos dentro vão 100% para MARONI

**Como melhorar:** aplicar tag `FINOPS-CLIENTE` nos Resource Groups dos clientes no portal Azure.

---

### AKS_OPENCOST

**O que é:** recursos de clusters Kubernetes compartilhados entre múltiplos clientes.

**Como funciona:**
1. OpenCost mede o custo de cada namespace dentro do cluster
2. Cada namespace é mapeado para um cliente via `clientes_alias.csv`
3. `Share_cliente = custo_namespaces_cliente / custo_total_cluster`
4. Namespaces de sistema (kube-system, etc.) são redistribuídos proporcionalmente

**Exemplos:**
- Cluster `aks-prd-newkmm` com 34 clientes → cada um paga conforme seus namespaces consomem

**Clusters cobertos:**

| Cluster | Tipo | Status |
|---|---|---|
| aks-prd-newkmm | Compartilhado | ✅ |
| aks-prd-br-newkmm | Compartilhado | ✅ |
| aks-hml-newkmm | Compartilhado | ✅ |
| aks-mg-kmm-firewall-eus-prd | Compartilhado | ✅ |
| aks-mg-kmm-firewall-eus-hml | Compartilhado | ✅ |
| aks-mg-kmm-dados-eus-prd | Compartilhado | ❌ OpenCost pendente |
| aks-prd-emissor | Compartilhado | ❌ OpenCost pendente |
| Clusters NORCOAST | Dedicados | N/A — entram como DEDICATED_TAG |

---

### SQLPOOL_CPU

**O que é:** SQL Elastic Pools compartilhados com databases de múltiplos clientes.

**Como funciona:**
1. Azure Monitor retorna a CPU média de cada database no pool
2. Databases com tag `FINOPS-CLIENTE` são mapeados ao cliente
3. `Share_cliente = soma_CPU_databases_cliente / soma_CPU_todos_databases_tagueados`
4. Databases sem tag têm custo redistribuído proporcionalmente entre os tagueados

**Exemplos:**
- Pool `sqldb-elastic-pool-vcore-prd-br` com databases de MARONI, ALIANCA e FSJ → cada um paga conforme CPU consumida

**Pools cobertos (maio/2026):**
- sqldb-elastic-pool-vcore-prd-br
- sqldb-elastic-pool-vcore-prd-eeua4
- sqldb-elastic-pool-vcore-prd-maroni-cw
- sqldb-elastic-pool-hml-eeua-vcore
- E demais pools com prefix `sqldb-elastic-pool`

---

### BACKUP_VAULT_ITEMS

**O que é:** Recovery Services Vaults centralizados que protegem recursos de múltiplos clientes.

**Como funciona:**
1. ARM API retorna a lista de protected items (VMs, Disks) do vault
2. Cada item é mapeado ao cliente pelo nome da VM ou tag do RG
3. `Share_cliente = items_protegidos_cliente / total_items_protegidos`

**Vaults cobertos:**
- `kmm-prd-default-bvault-us` (rg-kmm-backup) — vault PRD principal
- `vault-m41c2l5l` — vault DEV

---

### BACKUP_SNAPSHOT_DIRECT

**O que é:** snapshots de discos de VMs com o nome do cliente identificável no ResourceName.

**Como funciona:** extrai o nome da VM do snapshot e mapeia direto para o cliente via `clientes_alias.csv`. `Share = 1,0`.

**Exemplo:** `vm-maroni-prd-data-20260515` → mapeado para MARONI.

---

### WEIGHT_PROPORTIONAL

**O que é:** método de último recurso para recursos sem identificação de dono.

**Como funciona:**
1. Soma todos os custos de recursos sem dono identificado
2. Calcula o peso de cada cliente: `peso_cliente = custo_dedicado_cliente / custo_dedicado_total`
3. Distribui o custo proporcional pelo peso: `Share_cliente = peso_cliente`

**Quando aparece:**
- Recursos em RG com tag `COMPARTILHADO` (explícito — correto)
- Recursos em RG sem tag (implícito — pode indicar falta de tagueamento)
- Infraestrutura genuinamente compartilhada (Application Gateway, Redis, Key Vault, etc.)

**Como reduzir:** aplicar tags `FINOPS-CLIENTE` nos Resource Groups no portal Azure. Ver [MANUTENCAO.md](./MANUTENCAO.md).

---

## Dicionário de campos — fact_allocated_cost_YYYY-MM.parquet

> Arquivo mensal gerado pelo s07. Um arquivo por mês em `gold/fact_allocated_cost_consolidated/`. Cada linha representa o custo de um recurso Azure alocado para um cliente em uma data específica. Considerado fechado definitivamente após o retro_fix da primeira segunda-feira do mês seguinte.

Este é o arquivo consumido pelo Power BI. Cada linha representa o custo de um recurso Azure alocado para um cliente em uma data específica.

### Campos de identificação

| Campo | Tipo | Exemplo | Descrição |
|---|---|---|---|
| `Date` | Date | `2026-05-01` | Data de uso do recurso (UsageDate) |
| `ResourceId` | String | `/subscriptions/.../vm-norcoast-prd` | Identificador único do recurso na Azure (ARM Resource ID) |
| `ResourceName` | String | `vm-norcoast-prd` | Nome amigável do recurso |
| `ResourceGroupName` | String | `rg-norcoast-prd` | Resource Group onde o recurso está |
| `ResourceType` | String | `Virtual machine` | Tipo de recurso Azure (formato amigável do FOCUS) |
| `SubscriptionId` | String | `52d4423b-...` | GUID da subscription Azure onde o recurso está |
| `ServiceName` | String | `Virtual Machines` | Nome do serviço Azure (ex: Virtual Machines, SQL Database) |

### Campos de custo

| Campo | Tipo | Exemplo | Descrição | ⚠️ Atenção |
|---|---|---|---|---|
| `Cost` | Decimal | `514.90` | Custo bruto do recurso na Azure naquele dia (EffectiveCost do FOCUS). **Repetido para cada cliente no fan-out.** | ❌ Não somar no Power BI — fica inflado |
| `AllocatedCost` | Decimal | `31.22` | Custo real alocado para este cliente neste recurso nesta data. `AllocatedCost = Cost × Share` | ✅ Sempre usar este |
| `Share` | Decimal | `0.060630` | Fração do custo do recurso atribuída a este cliente. Soma dos shares de todos os clientes para um recurso = 1,0 | — |

### Campos de alocação

| Campo | Tipo | Exemplo | Descrição |
|---|---|---|---|
| `Cliente` | String | `NORCOAST` | Nome do cliente que recebe este custo |
| `AllocationMethod` | String | `DEDICATED_TAG` | Como o custo foi alocado. Ver tabela de métodos acima. |
| `ServiceKey` | String | `SQLPOOL:sqldb-elastic-pool-prd-br` | Identificador do pool/serviço que agrupa os recursos para rateio. Para recursos dedicados = ResourceId |
| `DriverType` | String | `SQLPOOL_DB_METRICS` | Origem dos dados que calcularam o Share. Ex: `SQLPOOL_DB_METRICS`, `OPENCOST_NAMESPACE`, `BACKUP_PROTECTED_ITEMS` |

---

## Valores possíveis de AllocationMethod

| Valor | Descrição | Share calculado por |
|---|---|---|
| `DEDICATED_TAG` | Recurso com tag direta ou herdada do RG | Tag FINOPS-CLIENTE |
| `AKS_OPENCOST` | Cluster AKS compartilhado | OpenCost (consumo real por namespace) |
| `SQLPOOL_CPU` | SQL Elastic Pool compartilhado | CPU por database (Azure Monitor) |
| `BACKUP_VAULT_ITEMS` | Recovery Services Vault | Número de protected items por cliente |
| `BACKUP_SNAPSHOT_DIRECT` | Snapshot de VM | Nome da VM no snapshot |
| `WEIGHT_PROPORTIONAL` | Sem identificação de dono | Peso financeiro do cliente |

---

## Valores possíveis de DriverType

| Valor | Descrição |
|---|---|
| `TAG_RESOURCE` | Tag no próprio recurso |
| `TAG_RG_INHERITED` | Tag herdada do Resource Group |
| `TAG_RG_SHARED` | RG tagueado como COMPARTILHADO |
| `OPENCOST_NAMESPACE` | Dados do OpenCost por namespace |
| `SQLPOOL_DB_METRICS` | CPU de databases via Azure Monitor |
| `BACKUP_PROTECTED_ITEMS` | Protected items via ARM API |
| `BACKUP_SNAPSHOT_NAME` | Nome da VM no ResourceName do snapshot |
| `WEIGHT_CLIENT_PROPORTION` | Peso financeiro proporcional |

---

## Manifest — manifest.csv

Tabela de controle com uma linha por data processada. Usada no dashboard de monitoramento do pipeline.

| Campo | Tipo | Descrição |
|---|---|---|
| `Date` | Date | Data processada |
| `Status` | String | `OK` = processado com sucesso \| `ERROR` = falha no processamento |
| `Rows` | Integer | Número de linhas geradas no fact_allocated_cost para esta data |
| `TotalAllocated` | Decimal | Soma de AllocatedCost do dia (= custo total Azure alocado) |
| `TotalCost` | Decimal | Custo bruto total do FOCUS para o dia (sem fan-out — valor de reconciliação) |
| `ProcessedAt` | String | Timestamp de quando o dia foi processado pelo pipeline |
| `PipelineRun` | String | ID do job Kubernetes que processou esta data |

> `TotalAllocated` deve sempre ser igual a `TotalCost` (tolerância < R$ 1,00). Se divergir, indica problema na alocação.

---

## Análise de qualidade — WP por categoria

O WEIGHT_PROPORTIONAL pode ser categorizado para identificar oportunidades de melhoria:

| Categoria | Significado | Ação |
|---|---|---|
| Infraestrutura compartilhada | Application Gateway, Redis, Bastion, Key Vault — genuinamente de todos | Correto — manter em WP |
| AKS infra WP | VMSS, LB, nós dos clusters — deveriam ser AKS_OPENCOST mas OpenCost não cobre | Instalar OpenCost nos clusters pendentes |
| Recursos sem tag (LTL, DADOS) | RGs de squads internos sem tag FINOPS-CLIENTE | Aplicar tag no RG do squad |
| Recursos sem tag (PRD) | VMs e serviços PRD sem identificação | Identificar dono e taguear |
| Recursos dedicados sem tag | Recurso claramente de um cliente mas sem tag | Taguear o recurso ou RG |
