# FinOps Azure Allocation Pipeline

Pipeline de alocação de custos Azure que transforma o FOCUS Report em custos distribuídos por cliente, com auditabilidade completa e integração com Power BI.

---

## Documentação

| Arquivo | Para quem | O que contém |
|---|---|---|
| [docs/VISAO_GERAL.md](./docs/VISAO_GERAL.md) | Todos | O que é o projeto, como funciona, glossário |
| [docs/MODELO_ALOCACAO.md](./docs/MODELO_ALOCACAO.md) | Dados / FinOps | Regras de rateio, métodos, dicionário de campos |
| [docs/POWER_BI.md](./docs/POWER_BI.md) | BI / Dados | Conexão, tabelas, medidas DAX, dicionário completo |
| [docs/TECNICO.md](./docs/TECNICO.md) | Engenharia | Deploy, scripts, infra, operação, manutenção |
| [docs/MANUTENCAO.md](./docs/MANUTENCAO.md) | Engenharia / FinOps | Como fazer mudanças: novo cliente, cluster, subscription |

---

## Estado atual (maio/2026 — cq18)

| Métrica | Valor |
|---|---|
| Pipeline | ✅ operacional (cq18, 2026-05-28) |
| Custo médio diário | ~R$ 14.062 |
| Total maio (25 dias) | R$ 351.552,92 |
| Clientes ativos | 91 |
| Linhas no consolidado maio | 707.805 |
| D-3 lag | ✅ configurado (`FOCUS_LAG_DAYS=3`) |
| Retrocarga semanal | ✅ toda segunda 07:00 UTC |
| s03d Backup | ✅ operacional — 2 vaults, BACKUP_VAULT_ITEMS ativo |
| s08 Quality Check | ✅ alertas diários em `gold/quality_alerts/` |
