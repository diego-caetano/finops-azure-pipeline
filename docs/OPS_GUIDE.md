# FinOps Pipeline — Guia de Operação

Comandos essenciais para investigar, diagnosticar e gerenciar o pipeline sem depender do Claude Code.

---

## Índice

1. [Conectar nos clusters](#1-conectar-nos-clusters)
2. [Jobs e CronJobs — status e histórico](#2-jobs-e-cronjobs--status-e-histórico)
3. [Pods — saúde e eventos](#3-pods--saúde-e-eventos)
4. [Logs — consultar e filtrar](#4-logs--consultar-e-filtrar)
5. [Disparar jobs manualmente](#5-disparar-jobs-manualmente)
6. [Alertas — investigar causa raiz](#6-alertas--investigar-causa-raiz)
7. [Blob Storage — verificar saídas](#7-blob-storage--verificar-saídas)
8. [Retrocarga e reconstrução](#8-retrocarga-e-reconstrução)
9. [Imagem Docker — build e rollback](#9-imagem-docker--build-e-rollback)
10. [Checklist diário rápido](#10-checklist-diário-rápido)

---

## 1. Conectar nos clusters

Cada cluster tem seu contexto kubectl separado. Sempre verificar em qual cluster você está antes de rodar comandos.

```bash
# Cluster central (pipeline principal + sync-rg-tags)
az aks get-credentials --resource-group RG-US-PRD-KMM --name aks-prd-newkmm --overwrite-existing

# Cluster BR PRD
az aks get-credentials --resource-group RG-PRD-KMM --name aks-prd-br-newkmm --overwrite-existing

# Cluster HML
az aks get-credentials --resource-group RG-US-PRD-KMM --name aks-hml-newkmm --overwrite-existing

# Cluster Firewall PRD
az aks get-credentials --resource-group RG-US-PRD-KMM --name aks-mg-kmm-firewall-eus-prd --overwrite-existing

# Cluster Firewall HML (subscription DEV-TEST — obrigatório especificar)
az aks get-credentials --resource-group RG-DEV-KMM --name aks-mg-kmm-firewall-eus-hml --subscription 3f6d197f-f70b-4c2c-b981-8bb575d47a7a --overwrite-existing

# Ver qual contexto está ativo
kubectl config current-context

# Listar todos os contextos disponíveis
kubectl config get-contexts

# Trocar de contexto sem reconectar
kubectl config use-context aks-prd-newkmm
```

---

## 2. Jobs e CronJobs — status e histórico

### Ver CronJobs e seu próximo disparo

```bash
# Todos os CronJobs do namespace finops
kubectl get cronjobs -n finops

# Com mais detalhes (última execução, schedule, suspenso?)
kubectl get cronjobs -n finops -o wide

# Ver schedule e próxima execução
kubectl get cronjob finops-pipeline -n finops -o jsonpath='{.spec.schedule}'

# Inspecionar um CronJob específico
kubectl describe cronjob finops-aks-allocation -n finops
kubectl describe cronjob finops-pipeline -n finops
kubectl describe cronjob finops-sync-rg-tags -n finops
```

### Ver histórico de Jobs

```bash
# Jobs ordenados por data de criação (mais recentes no fim)
kubectl get jobs -n finops --sort-by=.metadata.creationTimestamp

# Com status e duração
kubectl get jobs -n finops --sort-by=.metadata.creationTimestamp \
  -o custom-columns="NAME:.metadata.name,STATUS:.status.conditions[0].type,START:.status.startTime,COMPLETE:.status.completionTime"

# Só os jobs com falha
kubectl get jobs -n finops --field-selector status.failed=1

# Jobs das últimas 24h (filtra pelo nome que contém timestamp)
kubectl get jobs -n finops --sort-by=.metadata.creationTimestamp | tail -20

# Status resumido: quantos completaram vs falharam
kubectl get jobs -n finops -o json | \
  python3 -c "import json,sys; j=json.load(sys.stdin); \
  [print(i['metadata']['name'], 'OK' if i['status'].get('succeeded') else 'FAIL') for i in j['items']]"
```

### Identificar o job mais recente de cada tipo

```bash
# Último job do pipeline principal
kubectl get jobs -n finops --sort-by=.metadata.creationTimestamp \
  -o jsonpath='{.items[-1].metadata.name}'

# Último job AKS
kubectl get jobs -n finops -l app=finops-aks-allocation \
  --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}'

# Guardar o nome em variável (útil para encadear comandos)
LAST_PIPELINE=$(kubectl get jobs -n finops -l app=finops-pipeline \
  --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}')
echo $LAST_PIPELINE
```

---

## 3. Pods — saúde e eventos

### Listar pods e status

```bash
# Todos os pods do namespace finops
kubectl get pods -n finops

# Com informações extras (node, IP, tempo de vida)
kubectl get pods -n finops -o wide

# Acompanhar em tempo real (atualiza automaticamente)
kubectl get pods -n finops --watch

# Ver só os pods com problema
kubectl get pods -n finops --field-selector=status.phase!=Succeeded,status.phase!=Running
```

### Inspecionar um pod com falha

```bash
# Substituir <nome-do-pod> pelo pod real
kubectl describe pod <nome-do-pod> -n finops

# Seções importantes no output do describe:
#   Events: → mostra erros de pull de imagem, OOMKilled, etc.
#   State: → se Terminated, mostra Exit Code e Reason
#   Limits/Requests: → se OOMKilled, indica que o pod excedeu o limite de memória

# Ver exit code do pod
kubectl get pod <nome-do-pod> -n finops \
  -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}'

# Ver razão de término
kubectl get pod <nome-do-pod> -n finops \
  -o jsonpath='{.status.containerStatuses[0].state.terminated.reason}'
```

### Investigar OOMKilled (memória)

```bash
# Verificar se foi OOMKilled
kubectl get pod <nome-do-pod> -n finops \
  -o jsonpath='{.status.containerStatuses[0].lastState.terminated.reason}'
# Se retornar "OOMKilled", o pod foi morto por exceder o limite de memória

# Ver limite de memória configurado
kubectl get pod <nome-do-pod> -n finops \
  -o jsonpath='{.spec.containers[0].resources.limits.memory}'
```

### Verificar ServiceAccount e Workload Identity

```bash
# Confirmar que o SA existe com a anotação correta
kubectl get serviceaccount finops-workload-identity-sa -n finops \
  -o jsonpath='{.metadata.annotations}'

# Deve conter: azure.workload.identity/client-id=<CLIENT_ID>
# Se estiver vazio, a identity não está configurada neste cluster
```

---

## 4. Logs — consultar e filtrar

### Logs de um job/pod específico

```bash
# Logs do pod mais recente de um job
kubectl logs job/<nome-do-job> -n finops

# Seguir logs em tempo real (tail -f equivalente)
kubectl logs job/<nome-do-job> -n finops -f

# Últimas N linhas
kubectl logs job/<nome-do-job> -n finops --tail=100

# Se o pod já terminou (logs de um pod anterior)
kubectl logs <nome-do-pod> -n finops --previous
```

### Filtros essenciais por padrão de log

```bash
# Ver apenas erros e alertas
kubectl logs job/<nome-do-job> -n finops | grep -E "❌|⚠|ERROR|WARN"

# Ver progresso passo a passo (cada step do pipeline)
kubectl logs job/<nome-do-job> -n finops | grep -E "▶️|✅|s0[0-9]"

# Ver só o step s03b (AKS)
kubectl logs job/<nome-do-job> -n finops | grep "s03b"

# Ver alertas de qualidade (s08)
kubectl logs job/<nome-do-job> -n finops | grep -E "NAMESPACE_SEM_ALIAS|RECURSO_NAO_ALOCADO"

# Ver namespaces sem alias com detalhes completos (multi-linha)
kubectl logs job/<nome-do-job> -n finops | grep -A 50 "NAMESPACE_SEM_ALIAS"

# Ver uso de memória (RSS) reportado em cada step
kubectl logs job/<nome-do-job> -n finops | grep "RSS="

# Ver erros de autenticação Azure
kubectl logs job/<nome-do-job> -n finops | grep -iE "credential|token|auth|permission|403|401"

# Ver erros de blob storage
kubectl logs job/<nome-do-job> -n finops | grep -iE "blob|storage|upload|download|ResourceNotFound"

# Ver erros OpenCost
kubectl logs job/<nome-do-job> -n finops | grep -iE "opencost|9003|allocation"

# Ver tempo de execução de cada step (linhas de conclusão)
kubectl logs job/<nome-do-job> -n finops | grep "concluído"
```

### Investigar um step específico que falhou

```bash
# Ver tudo que aconteceu no s03b (AKS) incluindo contexto
kubectl logs job/<nome-do-job> -n finops | grep -B 2 -A 10 "s03b"

# Ver erro completo com traceback
kubectl logs job/<nome-do-job> -n finops | grep -A 30 "❌"

# Ver o momento exato da falha
kubectl logs job/<nome-do-job> -n finops | grep -E "ERROR|Exception|Traceback" -A 5
```

### Script helper — analisar job AKS em todos os clusters

```bash
# Rodar em sequência para cada cluster (ajustar RGs conforme necessário)
for CLUSTER in aks-prd-newkmm aks-prd-br-newkmm aks-hml-newkmm aks-mg-kmm-firewall-eus-prd aks-mg-kmm-firewall-eus-hml; do
  echo "=== $CLUSTER ==="
  kubectl config use-context $CLUSTER 2>/dev/null || echo "  contexto não disponível"
  LAST=$(kubectl get jobs -n finops -l app=finops-aks-allocation \
    --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}' 2>/dev/null)
  if [ -n "$LAST" ]; then
    STATUS=$(kubectl get job $LAST -n finops -o jsonpath='{.status.conditions[0].type}' 2>/dev/null)
    echo "  Último job: $LAST | Status: $STATUS"
    kubectl logs job/$LAST -n finops 2>/dev/null | grep -E "✅|❌|⚠" | tail -5
  else
    echo "  Nenhum job encontrado"
  fi
  echo ""
done
```

---

## 5. Disparar jobs manualmente

### Job único (teste ou reprocessamento emergencial)

```bash
# Pipeline principal (em aks-prd-newkmm)
az aks get-credentials --resource-group RG-US-PRD-KMM --name aks-prd-newkmm --overwrite-existing
kubectl create job --from=cronjob/finops-pipeline \
  finops-pipeline-manual-$(date +%Y%m%d-%H%M) -n finops

# Job AKS (em cada cluster — alterar contexto antes)
kubectl create job --from=cronjob/finops-aks-allocation \
  finops-aks-manual-$(date +%Y%m%d-%H%M) -n finops

# Sync RG tags (em aks-prd-newkmm)
kubectl create job --from=cronjob/finops-sync-rg-tags \
  finops-sync-manual-$(date +%Y%m%d-%H%M) -n finops

# Acompanhar execução em tempo real
kubectl logs -f job/<nome-do-job-criado> -n finops
```

### Job com data específica (reprocessar dia passado)

```bash
# Criar ConfigMap temporário com a data desejada
kubectl create configmap finops-override-date -n finops \
  --from-literal=PIPELINE_DATE=2026-06-15

# Criar job que usa o ConfigMap de override
# (alternativa mais simples: usar retro_fix diretamente — ver seção 8)
```

### Aguardar conclusão de um job

```bash
# Bloquear terminal até completar (timeout 10min)
kubectl wait --for=condition=complete --timeout=600s \
  job/<nome-do-job> -n finops

# Verificar resultado após conclusão
kubectl get job <nome-do-job> -n finops \
  -o jsonpath='{.status.conditions[0].type}'
# "Complete" = sucesso | "Failed" = falha
```

### Limpar jobs antigos (não remove CronJobs, apenas Jobs)

```bash
# Ver jobs com mais de 2 dias
kubectl get jobs -n finops --sort-by=.metadata.creationTimestamp | head -20

# Deletar job específico
kubectl delete job <nome-do-job> -n finops

# Deletar todos os jobs concluídos (cuidado: perde histórico de logs)
kubectl delete jobs -n finops --field-selector status.successful=1
```

---

## 6. Alertas — investigar causa raiz

### Alerta: finops-aks-jobs-incompletos (menos de 5 clusters concluíram)

```bash
# 1. Conectar em cada cluster e verificar o job mais recente
for CLUSTER in aks-prd-newkmm aks-prd-br-newkmm aks-hml-newkmm \
               aks-mg-kmm-firewall-eus-prd aks-mg-kmm-firewall-eus-hml; do
  echo "=== $CLUSTER ==="
  kubectl config use-context $CLUSTER
  kubectl get jobs -n finops --sort-by=.metadata.creationTimestamp | tail -3
  echo ""
done

# 2. Verificar pull de imagem (causa comum de falha em todos os clusters)
kubectl describe pod <nome-do-pod> -n finops | grep -A 5 "Events:"
# Procurar: "Failed to pull image" ou "ImagePullBackOff"

# 3. Verificar se o ACR está acessível
az acr check-health --name registrykmm --yes

# 4. Verificar se a imagem existe
az acr repository show-tags --name registrykmm --repository finops-pipeline

# 5. Verificar token da Workload Identity
kubectl get events -n finops --sort-by=.metadata.creationTimestamp | \
  grep -iE "error|fail|token" | tail -20
```

### Alerta: finops-namespaces-sem-alias (nomes não aparecem no e-mail)

```bash
# O log EXISTE mas o e-mail só captura a 1ª linha da mensagem multi-linha.
# Para ver os namespaces reais, consulte direto nos logs ou no blob:

# Opção 1: logs do pipeline principal (roda o s08)
LAST_PIPELINE=$(kubectl get jobs -n finops -l app=finops-pipeline \
  --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}')
kubectl logs job/$LAST_PIPELINE -n finops | grep -A 50 "NAMESPACE_SEM_ALIAS"

# Opção 2: baixar o CSV gerado pelo s08
DATA=$(date -u -d '-3 days' '+%Y-%m-%d')  # D-3 UTC  
az storage blob download \
  --account-name stpslkmmfinopseusprd \
  --container-name finops-v2 \
  --name "gold/quality_alerts/dt=${DATA}/namespaces_sem_alias_${DATA}.csv" \
  --file /tmp/ns_sem_alias.csv \
  --auth-mode login
cat /tmp/ns_sem_alias.csv

# Opção 3: listar os CSVs por cluster (gerados pelo s03b em cada cluster)
az storage blob list \
  --account-name stpslkmmfinopseusprd \
  --container-name finops-v2 \
  --prefix "gold/aks_allocation_share_opencost/dt=${DATA}/" \
  --auth-mode login \
  --query "[?contains(name, 'namespaces_sem_alias')].name" -o tsv
```

### Alerta: recursos não alocados

```bash
DATA=$(date -u -d '-3 days' '+%Y-%m-%d')

# Baixar o CSV de não alocados
az storage blob download \
  --account-name stpslkmmfinopseusprd \
  --container-name finops-v2 \
  --name "gold/quality_alerts/dt=${DATA}/recursos_nao_alocados_${DATA}.csv" \
  --file /tmp/nao_alocados.csv \
  --auth-mode login

# Top recursos por custo
sort -t';' -k2 -rn /tmp/nao_alocados.csv | head -20

# Ver no log do pipeline
kubectl logs job/$LAST_PIPELINE -n finops | grep -A 10 "RECURSO_NAO_ALOCADO"
```

---

## 7. Blob Storage — verificar saídas

### Manifest — saúde geral do consolidado

```bash
az storage blob download \
  --account-name stpslkmmfinopseusprd \
  --container-name finops-v2 \
  --name "gold/fact_allocated_cost_consolidated/manifest.csv" \
  --file /tmp/manifest.csv \
  --auth-mode login && cat /tmp/manifest.csv

# Verificar se todas as datas do mês atual estão com Status=OK
grep "$(date '+%Y-%m')" /tmp/manifest.csv | grep -v "OK"  # deve retornar vazio
```

### Listar arquivos de uma data específica

```bash
DATA=2026-06-16  # alterar conforme necessário

# Ver todos os outputs do pipeline para a data
az storage blob list \
  --account-name stpslkmmfinopseusprd \
  --container-name finops-v2 \
  --prefix "gold/fact_allocated_cost/dt=${DATA}/" \
  --auth-mode login \
  --query "[].{name:name, size:properties.contentLength}" -o table

# Ver arquivos de alocação AKS (gerados pelo s03b)
az storage blob list \
  --account-name stpslkmmfinopseusprd \
  --container-name finops-v2 \
  --prefix "gold/aks_allocation_share_opencost/dt=${DATA}/" \
  --auth-mode login --query "[].name" -o tsv

# Ver arquivos de qualidade (s08)
az storage blob list \
  --account-name stpslkmmfinopseusprd \
  --container-name finops-v2 \
  --prefix "gold/quality_alerts/dt=${DATA}/" \
  --auth-mode login --query "[].name" -o tsv
```

### Verificar se um arquivo existe antes de baixar

```bash
az storage blob exists \
  --account-name stpslkmmfinopseusprd \
  --container-name finops-v2 \
  --name "gold/fact_allocated_cost/dt=2026-06-16/fact_allocated_cost_2026-06-16.parquet" \
  --auth-mode login \
  --query "exists"
```

### Baixar e inspecionar arquivos CSV de referência

```bash
# clientes_alias.csv (mapeia namespaces para clientes)
az storage blob download \
  --account-name stpslkmmfinopseusprd --container-name finops-v2 \
  --name "config/reference/clientes_alias.csv" \
  --file /tmp/clientes_alias.csv --auth-mode login && cat /tmp/clientes_alias.csv

# cluster_config.csv (clusters AKS registrados)
az storage blob download \
  --account-name stpslkmmfinopseusprd --container-name finops-v2 \
  --name "config/reference/cluster_config.csv" \
  --file /tmp/cluster_config.csv --auth-mode login && cat /tmp/cluster_config.csv

# rg_tags.csv (tags sincronizadas pelo s00)
az storage blob download \
  --account-name stpslkmmfinopseusprd --container-name finops-v2 \
  --name "config/reference/rg_tags.csv" \
  --file /tmp/rg_tags.csv --auth-mode login
# Verificar tag de um RG específico:
grep -i "NOME-DO-RG" /tmp/rg_tags.csv
```

### Atualizar arquivo de referência no blob

```bash
# Editar localmente e depois fazer upload (substitui o arquivo no blob)
az storage blob upload \
  --account-name stpslkmmfinopseusprd \
  --container-name finops-v2 \
  --name "config/reference/clientes_alias.csv" \
  --file "config/reference/clientes_alias.csv" \
  --auth-mode login \
  --overwrite
```

---

## 8. Retrocarga e reconstrução

### Reprocessar dias específicos

```bash
# Reprocessar uma semana (rodar localmente com credenciais Azure)
python sh/retro_fix.py --start 2026-06-10 --end 2026-06-16

# Reprocessar um dia específico
python sh/retro_fix.py --start 2026-06-14 --end 2026-06-14

# Monitorar progresso (o script loga cada data processada)
```

### Reconstruir consolidado Power BI após retrocarga

```bash
az aks get-credentials --resource-group RG-US-PRD-KMM --name aks-prd-newkmm --overwrite-existing

kubectl delete job finops-fix-consolidated -n finops --ignore-not-found=true
kubectl apply -f k8s/job-fix-consolidated.yaml
kubectl wait --for=condition=complete --timeout=600s job/finops-fix-consolidated -n finops
kubectl logs job/finops-fix-consolidated -n finops | grep -E "s07|concluída|ERROR|linhas"
```

### Job de retrocarga via Kubernetes (para períodos longos)

```bash
# Editar job-retro.yaml com o período desejado antes de aplicar
kubectl apply -f k8s/job-retro.yaml
kubectl logs -f job/finops-retro -n finops
```

---

## 9. Imagem Docker — build e rollback

### Build e push da nova imagem

```bash
$env:PYTHONIOENCODING="utf-8"  # PowerShell
az acr build --registry registrykmm --image finops-pipeline:latest .

# Verificar se o build foi enviado
az acr repository show-tags --name registrykmm --repository finops-pipeline
```

### Forçar re-pull da imagem em todos os pods

```bash
# Os pods usam imagePullPolicy: Always — o próximo job já pega a nova imagem.
# Para forçar imediatamente sem aguardar o CronJob:
kubectl create job --from=cronjob/finops-aks-allocation finops-aks-test-$(date +%s) -n finops
```

### Rollback para imagem anterior

```bash
# Listar tags disponíveis no ACR
az acr repository show-tags --name registrykmm --repository finops-pipeline --orderby time_desc

# Se houver tag versionada (ex: :20260615), editar o ConfigMap ou o CronJob:
kubectl edit cronjob finops-pipeline -n finops
# Alterar: image: registrykmm.azurecr.io/finops-pipeline:20260615
```

---

## 10. Checklist diário rápido

Verificação matinal (roda tudo em aks-prd-newkmm):

```bash
az aks get-credentials --resource-group RG-US-PRD-KMM --name aks-prd-newkmm --overwrite-existing

# 1. Ver status dos jobs de hoje
kubectl get jobs -n finops --sort-by=.metadata.creationTimestamp | tail -10

# 2. Ver se o pipeline principal concluiu com sucesso
LAST=$(kubectl get jobs -n finops -l app=finops-pipeline \
  --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}')
kubectl get job $LAST -n finops
kubectl logs job/$LAST -n finops | grep -E "🏁|❌|s08"

# 3. Verificar alertas de qualidade nos logs do pipeline
kubectl logs job/$LAST -n finops | grep -E "NAMESPACE_SEM_ALIAS|RECURSO_NAO_ALOCADO"

# 4. Verificar manifest do consolidado
az storage blob download \
  --account-name stpslkmmfinopseusprd --container-name finops-v2 \
  --name "gold/fact_allocated_cost_consolidated/manifest.csv" \
  --file /tmp/manifest.csv --auth-mode login --overwrite && \
  tail -5 /tmp/manifest.csv
```

### Sinais de problema no manifest

| Campo | Normal | Problema |
|---|---|---|
| `Status` | `OK` | `ERROR` ou ausente |
| `TotalAllocated` | ~valor diário | `0` ou nulo |
| `Rows` | > 10.000 | < 1.000 |
| `TotalAllocated` vs `TotalCost` | diferença < R$1 | diferença > R$100 |

### Padrões de log para busca rápida

| O que procurar | Grep |
|---|---|
| Falha em qualquer step | `grep "❌"` |
| Alertas de qualidade | `grep -E "⚠ NAMESPACE\|⚠ RECURSO"` |
| Namespaces sem alias (lista completa) | `grep -A 50 "NAMESPACE_SEM_ALIAS"` |
| Passo a passo de execução | `grep -E "▶️\|✅"` |
| Uso de memória | `grep "RSS="` |
| Erros de autenticação | `grep -iE "credential\|403\|401"` |
| Erros de blob | `grep -iE "ResourceNotFound\|BlobNotFound"` |
| Conclusão do pipeline | `grep "🏁"` |

---

## Referências rápidas

| Recurso | Valor |
|---|---|
| Storage de saída | `stpslkmmfinopseusprd` / container `finops-v2` |
| Storage FOCUS (entrada) | `azurecostexportfocus10` |
| Namespace K8s | `finops` |
| Registro de imagem | `registrykmm.azurecr.io/finops-pipeline:latest` |
| Cluster central | `aks-prd-newkmm` / RG `RG-US-PRD-KMM` |
| Pipeline schedule | `06:00 UTC` diário |
| AKS jobs schedule | `05:00 UTC` diário (cada cluster) |
| Retro-fix schedule | `07:00 UTC` toda segunda-feira |
