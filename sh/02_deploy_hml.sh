#!/usr/bin/env bash
# =============================================================================
# 02_deploy_hml.sh
# Build + push da imagem Docker + deploy do CronJob no aks-hml-newkmm
#
# Managed Identity já criada manualmente:
#   Nome     : id-mg-kmm-finops-aks-newkmm-eus-hml
#   Client ID: 5eed8b89-7707-44cf-85e3-a6f86866a25f
#
# COMO USAR (terminal do VS Code dentro da pasta finops-pipeline):
#   bash 02_deploy_hml.sh
# =============================================================================

set -euo pipefail

GREEN='\033[0;32m'; BLUE='\033[0;34m'
YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
success() { echo -e "${GREEN}[OK]${NC}    $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
err()     { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
ACR_NAME="registrykmm"
IMAGE_NAME="finops-pipeline"
IMAGE_TAG="latest"
IMAGE_DATE_TAG="$(date +%Y%m%d)"
IMAGE_FULL="${ACR_NAME}.azurecr.io/${IMAGE_NAME}"

SUB_PIPELINE="52d4423b-7ed9-4673-b8e2-fa21cdb83176"
STORAGE_PIPELINE="stpslkmmfinopseusprd"
CONTAINER_V2="finops-v2"

CLUSTER_NAME="aks-hml-newkmm"
CLUSTER_RG="RG-US-PRD-KMM"
CLIENT_ID="5eed8b89-7707-44cf-85e3-a6f86866a25f"
K8S_NAMESPACE="finops"

# =============================================================================
# PASSO 0 — Verificar pré-requisitos
# =============================================================================
echo ""
info "=== PASSO 0 — Verificando pré-requisitos ==="

[ -f "k8s/cronjob-aks.yaml" ] || \
  err "k8s/cronjob-aks.yaml não encontrado. Execute na raiz do projeto finops-pipeline."

[ -f "Dockerfile" ] || \
  err "Dockerfile não encontrado. Execute na raiz do projeto finops-pipeline."

[ -d "config/reference" ] || \
  err "Pasta config/reference/ não encontrada."

[ -f "config/reference/cluster_config.csv" ] || \
  err "config/reference/cluster_config.csv não encontrado."

[ -f "config/reference/clientes_alias.csv" ] || \
  err "config/reference/clientes_alias.csv não encontrado."

[ -f "config/reference/aks_system_namespaces.csv" ] || \
  err "config/reference/aks_system_namespaces.csv não encontrado."

docker info &>/dev/null || \
  err "Docker não está rodando. Abra o Docker Desktop e tente novamente."

success "Pré-requisitos OK"
echo ""
info "Cluster  : $CLUSTER_NAME"
info "RG       : $CLUSTER_RG"
info "Client ID: $CLIENT_ID"
info "Imagem   : ${IMAGE_FULL}:${IMAGE_TAG}"

# =============================================================================
# PASSO 1 — Build da imagem Docker
# =============================================================================
echo ""
info "=== PASSO 1 — Build da imagem Docker ==="
info "Isso pode levar ~2 minutos na primeira vez ..."
echo ""

docker build \
  --tag "${IMAGE_FULL}:${IMAGE_TAG}" \
  --tag "${IMAGE_FULL}:${IMAGE_DATE_TAG}" \
  --file Dockerfile \
  .

echo ""
success "Build concluído"
info "Tags geradas:"
info "  ${IMAGE_FULL}:${IMAGE_TAG}"
info "  ${IMAGE_FULL}:${IMAGE_DATE_TAG}"

# =============================================================================
# PASSO 2 — Login no ACR e push
# =============================================================================
echo ""
info "=== PASSO 2 — Push para ACR $ACR_NAME ==="

az account set --subscription "$SUB_PIPELINE"
az acr login --name "$ACR_NAME"

info "Enviando imagem para o ACR ..."
docker push "${IMAGE_FULL}:${IMAGE_TAG}"
docker push "${IMAGE_FULL}:${IMAGE_DATE_TAG}"

echo ""
success "Push concluído"

# =============================================================================
# PASSO 3 — Upload dos arquivos config/reference para o Blob Storage
# =============================================================================
echo ""
info "=== PASSO 3 — Upload config/reference para Blob Storage ==="
info "Storage : $STORAGE_PIPELINE"
info "Container: $CONTAINER_V2"
echo ""

declare -a REF_FILES=(
  "cluster_config.csv"
  "clientes_alias.csv"
  "aks_system_namespaces.csv"
)

for FILE in "${REF_FILES[@]}"; do
  LOCAL="config/reference/$FILE"
  BLOB_PATH="config/reference/$FILE"

  info "Enviando $FILE ..."
  az storage blob upload \
    --account-name "$STORAGE_PIPELINE" \
    --container-name "$CONTAINER_V2" \
    --name "$BLOB_PATH" \
    --file "$LOCAL" \
    --auth-mode login \
    --overwrite true \
    --output none

  success "  $BLOB_PATH"
done

# =============================================================================
# PASSO 4 — Conectar no cluster e aplicar manifest
# =============================================================================
echo ""
info "=== PASSO 4 — Deploy no cluster $CLUSTER_NAME ==="

# Conecta no cluster
info "Obtendo credenciais do cluster ..."
az aks get-credentials \
  --resource-group "$CLUSTER_RG" \
  --name "$CLUSTER_NAME" \
  --overwrite-existing \
  --output none

CURRENT=$(kubectl config current-context)
success "Conectado: $CURRENT"

# Cria namespace finops (idempotente)
echo ""
info "Criando namespace $K8S_NAMESPACE ..."
kubectl create namespace "$K8S_NAMESPACE" \
  --dry-run=client -o yaml | kubectl apply -f -
success "Namespace $K8S_NAMESPACE OK"

# Gera YAML temporário substituindo os placeholders
echo ""
info "Preparando manifest (substituindo CLIENT_ID e ACR) ..."
YAML_TEMP=$(mktemp /tmp/cronjob-aks-hml-XXXXXX.yaml)

sed \
  -e "s|<CLIENT_ID>|${CLIENT_ID}|g" \
  -e "s|<ACR>|${ACR_NAME}|g" \
  k8s/cronjob-aks.yaml > "$YAML_TEMP"

# Mostra o diff do que será aplicado
echo ""
info "Recursos que serão criados/atualizados:"
grep "^kind:\|^  name:" "$YAML_TEMP" | sed 's/^/  /'

# Aplica o manifest
echo ""
info "Aplicando manifest no cluster ..."
kubectl apply -f "$YAML_TEMP"
rm -f "$YAML_TEMP"

# Confirma o que foi criado
echo ""
info "Recursos no namespace $K8S_NAMESPACE:"
kubectl get serviceaccount,configmap,cronjob -n "$K8S_NAMESPACE"

# =============================================================================
# PASSO 5 — Teste manual
# =============================================================================
echo ""
info "=== PASSO 5 — Teste manual ==="

JOB_NAME="finops-aks-test-$(date +%s)"
info "Criando job de teste: $JOB_NAME ..."

kubectl create job \
  --from=cronjob/finops-aks-allocation \
  "$JOB_NAME" \
  -n "$K8S_NAMESPACE"

success "Job criado: $JOB_NAME"
echo ""
info "Aguardando pod iniciar (20s) ..."
sleep 20

# Localiza o pod do job
POD=$(kubectl get pods -n "$K8S_NAMESPACE" \
  -l "job-name=$JOB_NAME" \
  --no-headers \
  -o custom-columns=":metadata.name" 2>/dev/null | head -1 || echo "")

if [ -n "$POD" ]; then
  POD_STATUS=$(kubectl get pod "$POD" -n "$K8S_NAMESPACE" \
    --no-headers -o custom-columns=":status.phase" 2>/dev/null || echo "Unknown")

  success "Pod encontrado: $POD (status: $POD_STATUS)"
  echo ""
  info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  info "LOGS DO JOB (Ctrl+C para sair — o job continua rodando)"
  info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""
  kubectl logs -f "$POD" -n "$K8S_NAMESPACE" || true
else
  warn "Pod ainda não disponível. Verifique manualmente:"
  echo ""
  echo "  # Ver pods do job"
  echo "  kubectl get pods -n $K8S_NAMESPACE -l job-name=$JOB_NAME"
  echo ""
  echo "  # Ver logs quando o pod estiver Running"
  echo "  kubectl logs -f -l job-name=$JOB_NAME -n $K8S_NAMESPACE"
  echo ""
  echo "  # Ver eventos do namespace (útil para diagnosticar falhas)"
  echo "  kubectl describe job $JOB_NAME -n $K8S_NAMESPACE"
fi

# =============================================================================
# RESUMO FINAL
# =============================================================================
echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}  Deploy HML concluído!${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""
echo "  Imagem publicada:"
echo "    ${IMAGE_FULL}:${IMAGE_TAG}"
echo "    ${IMAGE_FULL}:${IMAGE_DATE_TAG}"
echo ""
echo "  CronJob ativo em: $CLUSTER_NAME"
echo "  Schedule: 05:00 UTC (02:00 BRT) todo dia"
echo ""
echo "  Comandos úteis:"
echo ""
echo "  # Ver status do CronJob"
echo "  kubectl get cronjob finops-aks-allocation -n $K8S_NAMESPACE"
echo ""
echo "  # Disparar execução manual"
echo "  kubectl create job --from=cronjob/finops-aks-allocation finops-aks-manual-\$(date +%s) -n $K8S_NAMESPACE"
echo ""
echo "  # Ver todos os jobs executados"
echo "  kubectl get jobs -n $K8S_NAMESPACE"
echo ""
echo "  # Ver output no Blob Storage"
echo "  az storage blob list \\"
echo "    --account-name $STORAGE_PIPELINE \\"
echo "    --container-name $CONTAINER_V2 \\"
echo "    --prefix gold/aks_allocation_share_opencost/ \\"
echo "    --auth-mode login \\"
echo "    --output table"
