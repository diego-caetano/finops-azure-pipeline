# FinOps Pipeline — Arquitetura Python

## Visão geral

Pipeline de alocação de custos Azure, migrado de PowerShell + Azure Automation
para Python + Kubernetes CronJob.

## Fonte de dados

FOCUS Report (Cost Management Export) — substitui o Azure Cost Export legado.

## Camadas

| Camada | Prefixo Blob            | Descrição                        |
|--------|-------------------------|----------------------------------|
| Silver | silver/fact_cost/       | Custo normalizado por dia        |
| Silver | silver/resource_to_service/ | Mapeamento recurso → ServiceKey |
| Silver | silver/client_weights/  | Peso financeiro por cliente      |
| Gold   | gold/aks_allocation_share_opencost/ | Share AKS por cluster |
| Gold   | gold/allocation_share_final/ | Share consolidado             |
| Gold   | gold/fact_allocated_cost/ | Custo alocado final             |

## Jobs

| Job               | Schedule (UTC) | Clusters              |
|-------------------|----------------|-----------------------|
| finops-aks-allocation | 05:00      | Cada cluster shared   |
| finops-pipeline-main  | 06:00      | Um cluster (master)   |
