# Análise do Pipeline FinOps — 2026-05-14

---

## Correção aplicada — filtro de subscriptions (2026-05-16)

### Contexto

O escopo do pipeline é exclusivamente as subscriptions KMM:

| Subscription | Nome | ID |
|---|---|---|
| KMM PRD | Azure EA - KMM - PRD | `52d4423b-7ed9-4673-b8e2-fa21cdb83176` |
| KMM DEV-TEST | Azure EA - KMM - DEV-TEST | `3f6d197f-f70b-4c2c-b981-8bb575d47a7a` |

O FOCUS Report do Azure EA cobre todas as subscriptions do enrollment — não apenas as KMM. Sem filtro, o pipeline processava recursos de clientes como NSTECH, NORCOAST, UNILEVER etc., que **não devem ser objeto deste pipeline**.

### Bug identificado

Ao adicionar o filtro de subscriptions em `s01_ingest_focus.py`, a constante `_BILLING_SUBSCRIPTION_IDS` armazenava GUIDs puros:

```
52d4423b-7ed9-4673-b8e2-fa21cdb83176
3f6d197f-f70b-4c2c-b981-8bb575d47a7a
```

Porém o FOCUS parquet armazena a coluna `SubscriptionId` (mapeada de `SubAccountId`) no formato ARM completo:

```
/subscriptions/52d4423b-7ed9-4673-b8e2-fa21cdb83176
/subscriptions/3f6d197f-f70b-4c2c-b981-8bb575d47a7a
```

Resultado: o `.isin()` nunca encontrava correspondência — **todas as 16.546 linhas eram removidas**. O pipeline completava com 0 linhas e R$ 0,00 de custo.

### Correção aplicada

`finops/steps/s01_ingest_focus.py` — step 5 do método `run()`:

```python
# Antes (bug):
df_out["SubscriptionId"].str.lower().isin(_BILLING_SUBSCRIPTION_IDS)

# Depois (fix):
sub_ids_normalized = (
    df_out["SubscriptionId"]
    .str.lower()
    .str.replace(r"^.*/subscriptions/", "", regex=True)
)
df_out = df_out[sub_ids_normalized.isin(_BILLING_SUBSCRIPTION_IDS)].copy()
```

A normalização remove o prefixo `/subscriptions/` antes da comparação.

---

### Retrocarga — 2026-05-01 a 2026-05-15

Executada em 2026-05-16 via `sh/retro_may.py` — 15 jobs sequenciais em `aks-prd-newkmm`.
Todos os 15 dias foram processados com sucesso, escopo correto (apenas KMM PRD + DEV-TEST).

| Data | FactCost (R$) | AllocatedPct |
|------|--------------|-------------|
| 2026-05-01 | 12.019,66 | 100,0% |
| 2026-05-02 | 25.690,90 | 100,0% |
| 2026-05-03 | 23.679,10 | 100,0% |
| 2026-05-04 | 16.110,57 | 100,0% |
| 2026-05-05 | 13.393,46 | 100,0% |
| 2026-05-06 | 14.609,32 | 100,0% |
| 2026-05-07 | 18.831,97 | 100,0% |
| 2026-05-08 | 9.724,26 | 100,0% |
| 2026-05-09 | 16.936,25 | 100,0% |
| 2026-05-10 | 11.512,02 | 100,0% |
| 2026-05-11 | 11.806,51 | 100,0% |
| 2026-05-12 | 12.225,36 | 100,0% |
| 2026-05-13 | 12.191,29 | 100,0% |
| 2026-05-14 | 16.867,36 | 100,0% |
| 2026-05-15 | 4.726,31 | 100,0% |
| **Total** | **R$ 220.324,24** | **100,0%** |

> Nota: o custo de 2026-05-14 no escopo correto (R$ 16.867) é muito menor que o da análise original
> abaixo (R$ 118.923), pois aquela análise foi feita **antes do filtro de subscriptions** e incluía
> todos os clientes do enrollment Azure EA, não apenas os recursos KMM.

---

## Análise histórica — 2026-05-14 (antes do filtro de subscriptions)

> **ATENÇÃO:** esta análise foi gerada em 2026-05-16 com o pipeline sem o filtro de subscriptions.
> Os valores refletem o custo de **todos** os recursos do enrollment Azure EA, não apenas KMM.
> Use a tabela de retrocarga acima para os valores do escopo correto.

> Gerado em 2026-05-16 após execução manual do job `finops-pipeline-test-1778877220`
> no cluster `aks-prd-newkmm`, processando D-1 (2026-05-14).

---

## Resultado geral

| Métrica | Valor |
|---------|-------|
| TotalFactCost | R$ 118.923,31 |
| TotalAllocated | R$ 118.923,36 |
| AllocatedPct | **100,0%** |
| TotalUnallocated | R$ 0,00 |
| Clientes cobertos | 236 |
| Diferença (float rounding) | R$ 0,04 — abaixo do threshold |

**Comparativo com pipeline PS (2026-05-12):** 50,4% → **100,0%**

---

## Breakdown por método de alocação

| AllocationMethod | Custo | % |
|-----------------|-------|---|
| WEIGHT_PROPORTIONAL | R$ 75.736,04 | 63,7% |
| DEDICATED_TAG | R$ 43.187,32 | 36,3% |
| AKS_OPENCOST | R$ 0,00 | 0,0% |

```
Dedicado (DEDICATED_TAG):    R$  43.187  (36,3%)
Compartilhado (WEIGHT_PROP): R$  75.736  (63,7%)
```

---

## Top 15 clientes

| # | Cliente | Total (R$) | Dedicado (R$) | Compartilhado (R$) | Share |
|---|---------|-----------|--------------|-------------------|-------|
| 1 | NSTECH | 53.795,07 | 12.807,96 | 40.987,11 | 45,2% |
| 2 | NORCOAST | 6.236,31 | 1.652,99 | 4.583,31 | 5,2% |
| 3 | UNILEVER | 5.126,36 | 2.200,00 | 2.926,36 | 4,3% |
| 4 | MARONI | 4.149,69 | 1.927,83 | 2.221,87 | 3,5% |
| 5 | YPE | 3.854,18 | 1.245,76 | 2.608,42 | 3,2% |
| 6 | GRUPO_SC | 2.997,87 | 2.189,39 | 808,48 | 2,5% |
| 7 | DANONE | 1.908,69 | 858,23 | 1.050,46 | 1,6% |
| 8 | GPA | 1.749,00 | 765,72 | 983,28 | 1,5% |
| 9 | BRF | 1.372,38 | 703,70 | 668,67 | 1,2% |
| 10 | MARFRIG | 1.314,50 | 733,91 | 580,59 | 1,1% |
| 11 | LOGGI | 1.080,88 | 533,61 | 547,27 | 0,9% |
| 12 | MERCADO_LIVRE | 1.032,82 | 515,69 | 517,13 | 0,9% |
| 13 | ARCELOR_MITTAL | 990,15 | 475,00 | 515,16 | 0,8% |
| 14 | CARREFOUR | 977,21 | 485,98 | 491,22 | 0,8% |
| 15 | ULTRAGAZ | 895,04 | 359,29 | 535,76 | 0,8% |

> Top 5 clientes concentram **61,5%** do custo total.
> 50 clientes (21%) têm custo diário inferior a R$ 10.

---

## Insights e achados

### 1. AKS_OPENCOST = R$ 0,00

Os custos de VMs dos clusters AKS não aparecem nas MC_ RGs no FOCUS para esta data.
Eles aparecem como **Reserved Instances** (`/providers/microsoft.capacity/reservationorders/...`),
com valores individuais acima de R$ 3.000 cada.

Essas linhas não têm tag de cluster nem pertencem a RGs MC_, portanto são classificadas
como `RG_INFERRED → WEIGHT_PROPORTIONAL`. O s03b (OpenCost) está gerando shares
corretamente, mas não há custo de VM direto para aplicar o driver.

**Conclusão:** não é um bug de pipeline — é uma característica de como o Azure expõe
custos de instâncias reservadas no FOCUS Report. As reservas cobrem os VMs dos node pools,
e o custo de reserva aparece como item separado sem vínculo direto com o cluster.

---

### 2. SQL Elastic Pools — caindo em WEIGHT_PROPORTIONAL

Os pools aparecem nos top ServiceKeys (ex: `elasticpools/multiembarcador07`,
`ep-sql-vcore-us-prd-02`) mas com `AllocationMethod=WEIGHT_PROPORTIONAL` em vez
de `SQL_ELASTIC_POOL_CPU_DISK`.

O s03c terminou em ~1 segundo — rápido demais para consultar a ARM/Monitor API.
**Causa provável:** os databases do pool não têm a tag `FINOPS-CLIENTE`, então s03c
loga "nenhum database com tag → pool sem share" e retorna vazio para cada pool.

Os custos dos pools **são 100% alocados** via peso financeiro (WEIGHT_PROPORTIONAL),
mas sem a granularidade de utilização real (CPU/Disk) que o s03c proporciona.

---

### 3. NSTECH = 45,2% do total — alta dependência de rateio compartilhado

```
NSTECH dedicado:      R$ 12.808  (24% do total NSTECH)
NSTECH compartilhado: R$ 40.987  (76% do total NSTECH)
```

A maior parte do custo NSTECH vem do rateio proporcional — recursos internos em RGs
sem tag (`RG_INFERRED`) com o maior peso financeiro do pool compartilhado.
O valor final **não muda** com tagueação, mas a rastreabilidade melhora
(de WEIGHT_PROPORTIONAL para DEDICATED_TAG).

---

### 4. 5 clientes com nome numérico (problema de tag no Azure)

| Cliente | Custo diário (R$) |
|---------|------------------|
| 267 | 253,06 |
| 8 | 48,22 |
| 35 | 8,69 |
| 64 | 4,39 |
| 187 | 4,35 |

A tag `FINOPS-CLIENTE` desses recursos contém valores numéricos em vez de nomes de
clientes. O pipeline lê o que está na tag — a correção deve ser feita no portal Azure.

---

### 5. Long tail — 50 clientes com < R$ 10/dia

21% dos 236 clientes têm custo diário inferior a R$ 10. Combinação de:
- Clientes com poucos recursos dedicados (a maior parte do custo vem do rateio)
- Possíveis clientes inativos que ainda têm algum recurso alocado

Não é um problema de pipeline, mas pode ser relevante para revisão comercial.

---

## Ações recomendadas

| Prioridade | Ação | Responsável | Impacto |
|------------|------|-------------|---------|
| Alta | Taguear databases SQL com `FINOPS-CLIENTE` | Azure/DBA | s03c passa a usar CPU/Disk — alocação mais precisa para os SQL Pools |
| Alta | Investigar se Reserved Instances devem ser alocadas por ServiceKey ou cluster | FinOps | ~R$ 8.700/dia alocados com driver mais correto |
| Media | Taguear RGs internos NSTECH com `FINOPS-CLIENTE=NSTECH` | Azure/Ops | Melhora rastreabilidade dos R$ 40.987 compartilhados diários |
| Baixa | Corrigir 5 tags numéricas no portal Azure | Azure/Ops | Qualidade de dados nos relatórios |

---

## Saídas geradas no Blob Storage

```
finops-v2/gold/fact_allocated_cost/dt=2026-05-14/
  fact_allocated_cost_2026-05-14.parquet    — 2.444.641 linhas
  final_cost_by_client_2026-05-14.parquet  — 236 clientes
  final_cost_detailed_2026-05-14.parquet   — 885.578 linhas
  reconciliation_2026-05-14.csv
  unallocated_cost_2026-05-14.csv          — 301.964 linhas (SHARE_IS_ZERO, custo=0)
```
