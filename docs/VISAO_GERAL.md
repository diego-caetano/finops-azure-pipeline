# Visão Geral — FinOps Azure Allocation Pipeline

## O que é este projeto

O FinOps Allocation Pipeline é um sistema automatizado que responde à pergunta:

> **"Quanto cada cliente está custando na Azure, de fato?"**

Toda empresa com múltiplos clientes em cloud compartilha o mesmo desafio: a Azure cobra pela infraestrutura total, mas os clientes usam recursos em comum (clusters Kubernetes, SQL Pools, Gateways, Backups). Sem um sistema de rateio, é impossível saber o custo real por cliente.

Este pipeline processa diariamente todos os custos Azure e distribui cada centavo entre os clientes segundo regras de negócio claras e auditáveis.

---

## Como funciona — visão simplificada

```
Azure cobra R$ 14.000/dia
        ↓
Pipeline identifica cada recurso e seu dono
        ↓
Recursos dedicados → 100% para o cliente dono
Recursos compartilhados → rateados por consumo real
Recursos sem identificação → rateados por peso financeiro
        ↓
Power BI mostra: NORCOAST pagou R$ 2.800 hoje
                 MARONI pagou R$ 1.400 hoje
                 ... (91 clientes)
```

---

## Fluxo de dados

```
FOCUS Report (Azure)          Dados brutos de custo
      ↓
s01 Ingestão                  Filtra subscriptions KMM
      ↓
s02 Classificação             Identifica dono de cada recurso
      ↓
s03a Dedicados                Recursos com tag → 100% para o cliente
s03b AKS                      Clusters → rateio por namespace (OpenCost)
s03c SQL Pool                 Bancos → rateio por CPU (Azure Monitor)
s03d Backup                   Vaults/Snapshots → rateio por itens protegidos
      ↓
s04 Pesos                     Calcula peso de cada cliente para rateio residual
s05 Consolidação              Une todos os shares, valida 100% alocado
      ↓
s06 Alocação                  Aplica AllocatedCost = Custo × Share por cliente
      ↓
s07 Power BI                  Atualiza o consolidado histórico
      ↓
Power BI Desktop/Service      Dashboard executivo
```

---

## Schedules diários

| Horário (UTC) | O que roda | Por quê |
|---|---|---|
| 04:00 | Sincroniza tags dos Resource Groups | Captura mudanças de tag feitas no dia anterior |
| 05:00 | Coleta dados do OpenCost (AKS) | OpenCost precisa rodar antes do pipeline principal |
| 06:00 | Pipeline principal (D-3) | s01→s02→s03a→s03c→s03d→s04→s05→s06→s07→s08 — processa D-3 BRT |
| 07:00 (seg) | Retrocarga semanal (últimos 7 dias) | Corrige billing lag de RIs, Savings Plans e virada de mês |

> **Por que D-3?** A Azure demora até 3 dias para publicar todos os meters de custo de um dia (especialmente compute de VMs e SQL). Processar D-3 garante que 100% dos custos já foram publicados.

---

## Glossário

| Termo | Significado |
|---|---|
| **FOCUS Report** | Exportação de custos Azure no formato padrão FOCUS (FinOps Open Cost and Usage Specification). É a fonte oficial de billing. |
| **AllocatedCost** | Custo que foi atribuído a um cliente específico após o rateio. É a métrica principal — sempre usar esta no Power BI. |
| **Cost** | Custo bruto do recurso na Azure. É repetido para cada cliente no arquivo final — NÃO usar para somar no Power BI. |
| **Share** | Fração do custo de um recurso atribuída a um cliente. A soma dos shares de todos os clientes para um recurso é sempre 1,0 (100%). |
| **ServiceKey** | Identificador do serviço/pool sendo rateado. Ex: `SQLPOOL:sqldb-elastic-pool-prd-br`, `AKS:aks-prd-newkmm`, `VAULT:kmm-prd-default-bvault-us`. |
| **AllocationMethod** | Como o custo foi distribuído. Ver tabela completa em [MODELO_ALOCACAO.md](./MODELO_ALOCACAO.md). |
| **WEIGHT_PROPORTIONAL** | Método de último recurso — recurso sem identificação de dono, rateado pelo peso financeiro do cliente. |
| **DEDICATED_TAG** | Recurso tagueado diretamente para um cliente — 100% do custo vai para ele. |
| **D-3** | Data de processamento: sempre 3 dias antes de hoje. Ex: hoje é dia 28, o pipeline processa o dia 25. |
| **Billing lag** | Atraso da Azure em publicar todos os custos de um dia. Pode ser de 1 a 3 dias. |
| **EffectiveCost** | Custo após amortização de Reserved Instances. O pipeline usa este campo (não BilledCost) para distribuir corretamente o custo de RIs. |
| **Resource Group (RG)** | Agrupamento lógico de recursos na Azure. A tag `FINOPS-CLIENTE` no RG é herdada por todos os recursos dentro dele. |
| **OpenCost** | Ferramenta open-source instalada em cada cluster Kubernetes que mede o custo de cada namespace. |
| **Namespace (AKS)** | Divisão lógica dentro de um cluster Kubernetes. Cada cliente tem seus workloads em namespaces próprios. |
| **Protected Item (Backup)** | Recurso (VM, Disk) que está sendo protegido por um vault de backup. Usado para ratear o custo do vault. |
| **Retrocarga** | Reprocessamento de dias passados. Necessário quando há correção de bug ou billing lag prolongado. |
| **Fan-out** | Processo de multiplicar uma linha de custo por N clientes, cada um com seu Share. É como o rateio funciona internamente. |
| **Meter** | Unidade de cobrança da Azure. Um mesmo recurso pode ter múltiplos meters (ex: SQL Pool tem meters de vCore, License e Storage). |

---

## Princípios do modelo

1. **100% alocado:** todo centavo cobrado pela Azure é distribuído para algum cliente. Não existe custo "perdido".

2. **Auditabilidade:** cada linha do output registra como o custo foi alocado (`AllocationMethod`, `Rule`, `ServiceKey`). É possível rastrear qualquer valor até a origem.

3. **Dedicado tem prioridade:** se um recurso tem tag de cliente, ele nunca entra no rateio proporcional. A tag é soberana.

4. **Rateio por consumo real:** onde possível (AKS, SQL, Backup), o rateio usa dados reais de consumo — não estimativas.

5. **Peso como último recurso:** WEIGHT_PROPORTIONAL só é usado quando não há como identificar o dono. Quanto mais tags forem aplicadas na Azure, menor o percentual de WP.
