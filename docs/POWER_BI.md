# Power BI — Guia Completo

## Conexão ao consolidado

### Fonte principal — consolidado mensal (fact_allocated_cost_YYYY-MM.parquet)

1. Power BI Desktop → **Obter dados** → **Web**
2. Colar a URL:
   ```
   https://stpslkmmfinopseusprd.blob.core.windows.net/finops-v2/gold/fact_allocated_cost_consolidated/fact_allocated_cost_2026-05.parquet
   ```
   > ⚠️ O consolidado é **mensal**. Trocar `2026-05` pelo mês desejado. Para automatizar, usar parâmetro no Power Query: criar parâmetro `Mes` (texto, padrão `2026-05`) e referenciar na URL.
   ```
3. Autenticar com **Conta Organizacional** → entrar com conta `@nstech.com.br`
4. Clicar em **Transformar dados** (não Carregar direto)
5. No Power Query: confirmar tipos de coluna (ver tabela abaixo)
6. Renomear a tabela para `FactAllocatedCost`
7. **Fechar e Aplicar**

### Segunda fonte — manifest.csv

1. Power BI Desktop → **Obter dados** → **Web**
2. Colar a URL:
   ```
   https://stpslkmmfinopseusprd.blob.core.windows.net/finops-v2/gold/fact_allocated_cost_consolidated/manifest.csv
   ```
3. No Power Query: definir delimitador como `;` (ponto e vírgula)
4. Renomear para `Manifest`

---

## Tipos de coluna no Power Query

### FactAllocatedCost

| Coluna | Tipo no Power Query | Observação |
|---|---|---|
| `Date` | Date | |
| `ResourceId` | Texto | |
| `ResourceName` | Texto | |
| `ResourceGroupName` | Texto | |
| `ResourceType` | Texto | |
| `SubscriptionId` | Texto | |
| `ServiceName` | Texto | |
| `ServiceKey` | Texto | |
| `Cliente` | Texto | |
| `Cost` | Número Decimal | ⚠️ Não usar em visuais — ver nota abaixo |
| `Share` | Número Decimal | |
| `AllocatedCost` | Número Decimal | ✅ Campo principal |
| `AllocationMethod` | Texto | |
| `DriverType` | Texto | |

### Manifest

| Coluna | Tipo no Power Query | Observação |
|---|---|---|
| `Date` | Date | |
| `Status` | Texto | |
| `Rows` | Número Inteiro | |
| `TotalAllocated` | Número Decimal | Separador decimal: vírgula (pt-BR) |
| `TotalCost` | Número Decimal | Separador decimal: vírgula (pt-BR) |
| `ProcessedAt` | Texto | |
| `PipelineRun` | Texto | |

> ⚠️ Se aparecer erro ao converter `TotalAllocated` para Decimal, usar **Usar Localidade → Português (Brasil)** para garantir que a vírgula seja interpretada como separador decimal.

---

## Tabela de datas (DimDate)

Criar via DAX para habilitar filtros de período:

```dax
DimDate =
CALENDAR(DATE(2026, 5, 1), TODAY())
```

Colunas calculadas na `DimDate`:

```dax
Year        = YEAR([Date])
MonthNum    = MONTH([Date])
MonthName   = FORMAT([Date], "MMM")
MonthYear   = FORMAT([Date], "MMM/yy")
Weekday     = FORMAT([Date], "ddd")
WeekdayNum  = WEEKDAY([Date], 2)
```

**Relacionamento:** `DimDate[Date]` → `FactAllocatedCost[Date]` (1:N)

---

## Tabela de medidas (_Medidas)

Criar uma tabela separada para organizar as medidas: **Modelagem → Nova Tabela** → nomear `_Medidas`.

### Medidas essenciais

```dax
// ✅ MEDIDA PRINCIPAL — sempre usar esta para custo
Total Alocado =
SUM(FactAllocatedCost[AllocatedCost])

// Custo médio por dia no período filtrado
Custo Médio Diário =
AVERAGEX(
    VALUES(FactAllocatedCost[Date]),
    CALCULATE(SUM(FactAllocatedCost[AllocatedCost]))
)

// Projeção para 30 dias com base na média atual
Projeção Mensal =
[Custo Médio Diário] * 30

// Número de dias processados no período
Dias Processados =
DISTINCTCOUNT(FactAllocatedCost[Date])

// Número de clientes distintos no período
Clientes Ativos =
DISTINCTCOUNT(FactAllocatedCost[Cliente])
```

### Medidas de participação

```dax
// % do cliente no total — respeita todos os filtros ativos
% Custo Cliente =
DIVIDE(
    SUM(FactAllocatedCost[AllocatedCost]),
    CALCULATE(
        SUM(FactAllocatedCost[AllocatedCost]),
        REMOVEFILTERS(FactAllocatedCost[Cliente])
    )
)

// % por método de alocação
% Método =
DIVIDE(
    SUM(FactAllocatedCost[AllocatedCost]),
    CALCULATE(
        SUM(FactAllocatedCost[AllocatedCost]),
        REMOVEFILTERS(FactAllocatedCost[AllocationMethod])
    )
)
```

### Medidas de variação

```dax
// Custo do mesmo período no dia anterior
Custo Dia Anterior =
CALCULATE(
    [Total Alocado],
    DATEADD(DimDate[Date], -1, DAY)
)

// Variação absoluta dia a dia
Variação Diária =
[Total Alocado] - [Custo Dia Anterior]

// Variação percentual dia a dia
% Variação Diária =
DIVIDE([Variação Diária], [Custo Dia Anterior])
```

### Medidas de monitoramento (Manifest)

```dax
// Datas processadas com sucesso
Datas OK =
CALCULATE(COUNTROWS(Manifest), Manifest[Status] = "OK")

// Datas com erro
Datas Erro =
CALCULATE(COUNTROWS(Manifest), Manifest[Status] = "ERROR")

// Total de linhas no consolidado
Total Linhas =
SUM(Manifest[Rows])
```

---

## Estrutura das 3 páginas

### Página 1 — Overview

**Objetivo:** visão executiva do custo total, evolução diária e ranking de clientes.

| Visual | Tipo | Campos | Justificativa |
|---|---|---|---|
| Total Alocado (mês) | Cartão | `Total Alocado` | Custo total do período — número mais importante do dashboard |
| Custo Médio Diário | Cartão | `Custo Médio Diário` | Referência para identificar anomalias no dia a dia |
| Projeção Mensal | Cartão | `Projeção Mensal` | Permite antecipar o fechamento do mês vs budget |
| Clientes Ativos | Cartão | `Clientes Ativos` | Controle de quantos clientes estão gerando custo |
| Custo por dia | Gráfico de linhas | Eixo X: `Date` \| Valor: `Total Alocado` | Identifica tendências, picos e anomalias de custo |
| Top clientes | Barras horizontais | Eixo: `Cliente` \| Valor: `Total Alocado` \| Ordenar desc | Hierarquia de custo por cliente — base para chargeback |
| Top recursos | Tabela | `ResourceName`, `ResourceGroupName`, `AllocationMethod`, `Total Alocado` | Drill-down dos recursos mais custosos |
| Filtro de data | Segmentador | `Date` (intervalo) | Permite selecionar período de análise |
| Filtro de cliente | Segmentador | `Cliente` (lista) | Isola a visão de um cliente específico |

---

### Página 2 — Allocation Methods

**Objetivo:** entender como o custo foi distribuído entre os métodos de alocação e identificar oportunidades de melhoria de tagueamento.

| Visual | Tipo | Campos | Justificativa |
|---|---|---|---|
| % DEDICATED_TAG | Cartão | `Total Alocado` filtrado por DEDICATED_TAG | Mostra quanto do custo tem dono certo — quanto maior, melhor |
| % WEIGHT_PROPORTIONAL | Cartão | `Total Alocado` filtrado por WP | Custo sem dono identificado — quanto menor, melhor |
| % AKS_OPENCOST | Cartão | `Total Alocado` filtrado por AKS | Custo de clusters rateado por consumo real |
| % SQLPOOL_CPU | Cartão | `Total Alocado` filtrado por SQL | Custo de SQL rateado por CPU |
| Distribuição por método | Gráfico de rosca | Legenda: `AllocationMethod` \| Valor: `Total Alocado` | Proporção visual de cada método |
| Evolução por método | Barras empilhadas | Eixo: `Date` \| Série: `AllocationMethod` \| Valor: `Total Alocado` | Identifica mudanças na composição ao longo do tempo |
| Custo dedicado vs compartilhado por cliente | Tabela | `Cliente`, custo DEDICATED_TAG, custo WP, `% Custo Cliente` | Permite ao cliente entender o que é seu e o que é rateado |
| Detalhe por método | Tabela | `AllocationMethod`, `ServiceKey`, `Cliente`, `Total Alocado`, `% Método` | Auditoria de como cada pool/cluster está sendo rateado |
| Filtro de método | Segmentador | `AllocationMethod` | Isola análise por método |
| Filtro de data | Segmentador | `Date` | Período de análise |

**Cores sugeridas por método** (configurar em "Cores de dados"):
- DEDICATED_TAG → `#F4400A` (laranja nstech)
- AKS_OPENCOST → `#2B7A4B` (verde)
- SQLPOOL_CPU → `#0D0F2B` (azul escuro)
- WEIGHT_PROPORTIONAL → `#C8C8C8` (cinza)
- BACKUP_VAULT_ITEMS → `#FF6B35` (laranja claro)

---

### Página 3 — Pipeline Monitor

**Objetivo:** monitorar a saúde do pipeline — garantir que todos os dias foram processados e os valores são consistentes.

| Visual | Tipo | Campos | Justificativa |
|---|---|---|---|
| Datas processadas | Cartão | `Datas OK` (Manifest) | Quantos dias têm dados válidos |
| Datas com erro | Cartão | `Datas Erro` (Manifest) | Alerta — qualquer valor > 0 exige investigação |
| Total linhas | Cartão | `Total Linhas` (Manifest) | Sanidade do volume de dados |
| Linhas por data | Barras | Eixo: `Date` (Manifest) \| Valor: `Rows` | Identifica dias com volume anormal (muito alto ou zero) |
| TotalCost vs TotalAllocated | Linhas duplas | Eixo: `Date` \| Linhas: `TotalCost` e `TotalAllocated` | As duas linhas devem ser sobrepostas — desvio indica problema de alocação |
| Histórico de execuções | Tabela | `Date`, `Status`, `Rows`, `TotalAllocated`, `TotalCost`, `ProcessedAt` | Log de auditoria — permite identificar quando e como cada dia foi processado |

**Formatação condicional na tabela — coluna Status:**
- `OK` → fundo `#E1F5EE`, texto `#0F6E56` (verde)
- `ERROR` → fundo `#FAECE7`, texto `#993C1D` (vermelho)

---

## Tema nstech

Aplicar o arquivo `nstech_powerbi_theme.json` via:
**Exibição → Temas → Procurar temas**

Paleta de cores:
```
#F4400A  laranja principal
#2D2D2D  cinza escuro
#C8C8C8  cinza claro
#FF6B35  laranja médio
#0D0F2B  azul escuro
#F5F5F5  fundo
#FFFFFF  branco
Fonte: Barlow
```

---

## Dicionário para o time de dados — base para views

Esta seção detalha cada campo para criação de views analíticas.

### View: vw_custo_por_cliente_dia

**Objetivo:** base para relatórios executivos e chargeback mensal.

```sql
SELECT
    Date,
    Cliente,
    SUM(AllocatedCost)          AS TotalAlocado,
    COUNT(DISTINCT ResourceId)  AS QtdRecursos,
    -- Participação do cliente no total do dia
    SUM(AllocatedCost) / SUM(SUM(AllocatedCost)) OVER (PARTITION BY Date)
                                AS PctDoDia
FROM fact_allocated_cost
WHERE AllocationMethod IS NOT NULL
GROUP BY Date, Cliente
ORDER BY Date, TotalAlocado DESC
```

---

### View: vw_custo_dedicado_vs_compartilhado

**Objetivo:** transparência para o cliente — quanto é dedicado (exclusivo dele) vs compartilhado (rateado).

```sql
SELECT
    Date,
    Cliente,
    SUM(CASE WHEN AllocationMethod = 'DEDICATED_TAG'
             THEN AllocatedCost ELSE 0 END)    AS CustoDedicado,
    SUM(CASE WHEN AllocationMethod = 'WEIGHT_PROPORTIONAL'
             THEN AllocatedCost ELSE 0 END)    AS CustoCompartilhadoWP,
    SUM(CASE WHEN AllocationMethod = 'AKS_OPENCOST'
             THEN AllocatedCost ELSE 0 END)    AS CustoAKS,
    SUM(CASE WHEN AllocationMethod = 'SQLPOOL_CPU'
             THEN AllocatedCost ELSE 0 END)    AS CustoSQL,
    SUM(CASE WHEN AllocationMethod = 'BACKUP_VAULT_ITEMS'
             THEN AllocatedCost ELSE 0 END)    AS CustoBackup,
    SUM(AllocatedCost)                         AS TotalAlocado
FROM fact_allocated_cost
GROUP BY Date, Cliente
```

---

### View: vw_recursos_sem_tag

**Objetivo:** governança — identificar recursos que estão em WEIGHT_PROPORTIONAL mas deveriam ter dono.

```sql
SELECT
    ResourceId,
    ResourceName,
    ResourceGroupName,
    ResourceType,
    ServiceName,
    SubscriptionId,
    SUM(AllocatedCost) / COUNT(DISTINCT Cliente) AS CustoBruto,
    COUNT(DISTINCT Cliente)                       AS QtdClientesPagando,
    MIN(Date)                                     AS PrimeiraOcorrencia,
    MAX(Date)                                     AS UltimaOcorrencia
FROM fact_allocated_cost
WHERE AllocationMethod = 'WEIGHT_PROPORTIONAL'
GROUP BY ResourceId, ResourceName, ResourceGroupName,
         ResourceType, ServiceName, SubscriptionId
ORDER BY CustoBruto DESC
```

> Use esta view para priorizar o tagueamento — os recursos no topo têm maior impacto financeiro.

---

### View: vw_auditoria_alocacao

**Objetivo:** rastreabilidade — para qualquer custo, mostrar exatamente como foi alocado.

```sql
SELECT
    f.Date,
    f.Cliente,
    f.ResourceName,
    f.ResourceGroupName,
    f.ResourceType,
    f.ServiceKey,
    f.AllocationMethod,
    f.DriverType,
    f.Cost         AS CustoBrutoRecurso,
    f.Share        AS FracaoAlocada,
    f.AllocatedCost AS CustoAlocado,
    -- Flags de qualidade
    CASE WHEN f.AllocationMethod = 'DEDICATED_TAG' THEN 'Dedicado'
         WHEN f.AllocationMethod = 'WEIGHT_PROPORTIONAL' THEN 'Proporcional'
         ELSE 'Rateio por consumo'
    END AS TipoAlocacao,
    CASE WHEN f.AllocationMethod = 'WEIGHT_PROPORTIONAL'
              AND f.ResourceName LIKE '%cliente%' THEN 'Revisar tag'
         ELSE 'OK'
    END AS FlagQualidade
FROM fact_allocated_cost f
```

---

### View: vw_evolucao_mensal_cliente

**Objetivo:** tendência de custo por cliente — base para forecast e alertas.

```sql
SELECT
    FORMAT(Date, 'yyyy-MM')  AS AnoMes,
    Cliente,
    SUM(AllocatedCost)        AS TotalMes,
    AVG(AllocatedCost)        AS MediaDiaria,
    MIN(AllocatedCost)        AS MinDiario,
    MAX(AllocatedCost)        AS MaxDiario,
    COUNT(DISTINCT Date)      AS DiasProcessados
FROM fact_allocated_cost
GROUP BY FORMAT(Date, 'yyyy-MM'), Cliente
ORDER BY AnoMes, TotalMes DESC
```

---

## Regra de ouro — Power BI

> ⚠️ **Sempre usar `SUM(AllocatedCost)`. Nunca `SUM(Cost)`.**
>
> O campo `Cost` é o custo bruto do recurso repetido para cada cliente no fan-out. Se um recurso custa R$ 514,90 e tem 33 clientes, `SUM(Cost)` retorna R$ 16.991,70 — 33x o valor real. `SUM(AllocatedCost)` retorna R$ 514,90 — o custo correto distribuído entre os clientes.
