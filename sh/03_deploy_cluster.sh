#!/usr/bin/env bash
# =============================================================================
# 03_deploy_cluster.sh
# Provisiona identidade + permissões + CronJob AKS para um cluster compartilhado.
# Imagem já está no ACR — não faz build/push.
#
# COMO USAR:
#   export CLUSTER_NAME="aks-prd-newkmm"
#   export CLUSTER_RG="RG-US-PRD-KMM"
#   export IDENTITY_NAME="id-mg-kmm-finops-aks-prd-newkmm-eus-prd"
#   bash sh/03_deploy_cluster.sh
#
# Clusters e seus parâmetros (nomes seguem taxonomia padrão KMM):
#   CLUSTER                       | CLUSTER_RG      | IDENTITY_NAME
#   aks-prd-newkmm                | RG-US-PRD-KMM   | id-mg-kmm-finops-aks-prd-newkmm-eus-prd
#   aks-prd-br-newkmm             | RG-PRD-KMM      | id-mg-kmm-finops-aks-prd-br-newkmm-eus-prd
#   aks-mg-kmm-firewall-eus-prd   | RG-US-PRD-KMM   | id-mg-kmm-finops-aks-firewall-eus-prd
#   aks-mg-kmm-firewall-eus-hml   | RG-DEV-KMM      | id-mg-kmm-finops-aks-firewall-eus-hml
# =============================================================================

set -euo pipefail

GREEN='\033[0;32m'; BLUE='\033[0;34m'
YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
success() { echo -e "${GREEN}[OK]${NC}    $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
err()     { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

# =============================================================================
# PARÂMETROS — editar ou exportar antes de executar
# =============================================================================
CLUSTER_NAME="${CLUSTER_NAME:?Defina CLUSTER_NAME antes de executar}"
CLUSTER_RG="${CLUSTER_RG:?Defina CLUSTER_RG antes de executar}"
IDENTITY_NAME="${IDENTITY_NAME:?Defina IDENTITY_NAME antes de executar}"
# SUB_CLUSTER: subscription onde vivem o cluster e a identity.
# Deixar vazio para usar a subscription ativa (maioria dos clusters).
# Necessário para aks-mg-kmm-firewall-eus-hml (sub KMM DEV-TEST).
SUB_CLUSTER="${SUB_CLUSTER:-}"

# Constantes do projeto
ACR_NAME="registrykmm"
ACR_RG="KMM-Registry"
SUB_FOCUS="0535039d-8b8e-4ac1-9fce-9f727e8ba2b1"
SUB_PIPELINE="52d4423b-7ed9-4673-b8e2-fa21cdb83176"
STORAGE_FOCUS="azurecostexportfocus10"
STORAGE_PIPELINE="stpslkmmfinopseusprd"
CONTAINER_V2="finops-v2"
K8S_NAMESPACE="finops"
IDENTITY_LOCATION="eastus"

# Tags obrigatórias CCoE — FINOPS-AMBIENTE derivado do nome do cluster
if [[ "$CLUSTER_NAME" == *"-hml"* || "$CLUSTER_NAME" == *"-hml-"* ]]; then
  FINOPS_AMBIENTE="HML"
elif [[ "$CLUSTER_NAME" == *"-prd"* || "$CLUSTER_NAME" == *"-prd-"* ]]; then
  FINOPS_AMBIENTE="PRD"
else
  FINOPS_AMBIENTE="${FINOPS_AMBIENTE:-PRD}"
fi
FINOPS_BUTORRE="PSL"
FINOPS_CLIENTE="COMPARTILHADO"
FINOPS_EMPRESA="KMM"
CREATED_ON="$(date +%d/%m/%Y)"

# Role Definition IDs (imutáveis no Azure)
ROLE_BLOB_READER="2a2b9908-6ea1-4ae2-8e65-a410df84e7d1"
ROLE_BLOB_CONTRIBUTOR="ba92f5b4-2d11-453d-a403-e96b0029c9fe"
ROLE_ACR_PULL="7f951dda-4ed3-4680-a7ca-43fe172d538d"

# =============================================================================
# FUNÇÕES
# =============================================================================
new_uuid() { python -c "import uuid; print(uuid.uuid4())"; }

PENDING_PERMS=()

assign_role() {
  local scope="$1"
  local role_def_id="$2"
  local principal_id="$3"
  local sub="$4"
  local label="$5"

  info "Atribuindo $label ..."

  local existing
  existing=$(az role assignment list \
    --assignee "$principal_id" \
    --scope "$scope" \
    --query "[?roleDefinitionId && ends_with(roleDefinitionId,'$role_def_id')].id" \
    --output tsv 2>/dev/null || echo "")

  if [ -n "$existing" ]; then
    warn "  $label já existe — pulando"
    return 0
  fi

  local uuid
  uuid=$(new_uuid)

  local url="https://management.azure.com${scope}/providers/Microsoft.Authorization/roleAssignments/${uuid}?api-version=2022-04-01"
  local body="{\"properties\":{\"roleDefinitionId\":\"/subscriptions/${sub}/providers/Microsoft.Authorization/roleDefinitions/${role_def_id}\",\"principalId\":\"${principal_id}\",\"principalType\":\"ServicePrincipal\"}}"

  local result
  if az rest --method put --url "$url" --body "$body" --output none 2>/tmp/az_rest_err; then
    success "  $label atribuído"
  else
    local err_msg
    err_msg=$(cat /tmp/az_rest_err)
    if echo "$err_msg" | grep -q "RoleAssignmentExists"; then
      warn "  $label já existe — pulando"
    elif echo "$err_msg" | grep -q "AuthorizationFailed\|does not have authorization"; then
      warn "  SEM PERMISSÃO para $label — adicionar manualmente:"
      warn "  az rest --method put \\"
      warn "    --url \"$url\" \\"
      warn "    --body '$body'"
      PENDING_PERMS+=("$label | OBJECT_ID=$principal_id | SCOPE=$scope")
    else
      echo "$err_msg" >&2
      return 1
    fi
  fi
}

# =============================================================================
# PASSO 0 — Pré-requisitos
# =============================================================================
echo ""
info "=== PASSO 0 — Verificando pré-requisitos ==="

[ -f "k8s/cronjob-aks.yaml" ] || \
  err "k8s/cronjob-aks.yaml não encontrado. Execute na raiz do projeto finops-pipeline."

command -v az   &>/dev/null || err "az CLI não encontrado."
command -v kubectl &>/dev/null || err "kubectl não encontrado."
command -v python &>/dev/null || err "python não encontrado."

success "Pré-requisitos OK"
echo ""
info "Cluster  : $CLUSTER_NAME"
info "RG       : $CLUSTER_RG"
info "Identity : $IDENTITY_NAME"
info "Ambiente : $FINOPS_AMBIENTE"

# =============================================================================
# PASSO 1 — Criar Managed Identity (idempotente)
# =============================================================================
echo ""
info "=== PASSO 1 — Managed Identity ==="

[ -n "$SUB_CLUSTER" ] && az account set --subscription "$SUB_CLUSTER" --output none

EXISTING_ID=$(az identity show \
  --name "$IDENTITY_NAME" \
  --resource-group "$CLUSTER_RG" \
  --query "clientId" -o tsv 2>/dev/null || echo "")

if [ -n "$EXISTING_ID" ]; then
  warn "Identity '$IDENTITY_NAME' já existe (clientId: $EXISTING_ID)"
  CLIENT_ID="$EXISTING_ID"
else
  info "Criando identity '$IDENTITY_NAME' em $CLUSTER_RG (AMBIENTE=$FINOPS_AMBIENTE) ..."
  az identity create \
    --name "$IDENTITY_NAME" \
    --resource-group "$CLUSTER_RG" \
    --location "$IDENTITY_LOCATION" \
    --tags \
      "FINOPS-AMBIENTE=${FINOPS_AMBIENTE}" \
      "FINOPS-BUTORRE=${FINOPS_BUTORRE}" \
      "FINOPS-CLIENTE=${FINOPS_CLIENTE}" \
      "FINOPS-EMPRESA=${FINOPS_EMPRESA}" \
      "Created-On=${CREATED_ON}" \
    --output none
  success "Identity criada"
  CLIENT_ID=$(az identity show \
    --name "$IDENTITY_NAME" \
    --resource-group "$CLUSTER_RG" \
    --query "clientId" -o tsv)
fi

OBJECT_ID=$(az identity show \
  --name "$IDENTITY_NAME" \
  --resource-group "$CLUSTER_RG" \
  --query "principalId" -o tsv)

info "CLIENT_ID : $CLIENT_ID"
info "OBJECT_ID : $OBJECT_ID"

# PASSO 2 (FOCUS Reader) removido: o job AKS não acessa o FOCUS storage.
# O FOCUS Reader só é necessário para o cronjob-pipeline.yaml (run_pipeline.py).

# =============================================================================
# PASSO 2 — Storage Blob Data Contributor no finops-v2
# =============================================================================
echo ""
[ -n "$SUB_CLUSTER" ] && az account set --subscription "$SUB_PIPELINE" --output none

info "=== PASSO 2 — Storage Blob Data Contributor (finops-v2) ==="

SCOPE_V2="/subscriptions/${SUB_PIPELINE}/resourceGroups/rg-psl-kmm-finops-eus-prd/providers/Microsoft.Storage/storageAccounts/${STORAGE_PIPELINE}/blobServices/default/containers/${CONTAINER_V2}"

assign_role "$SCOPE_V2" "$ROLE_BLOB_CONTRIBUTOR" "$OBJECT_ID" "$SUB_PIPELINE" \
  "Storage Blob Data Contributor em $CONTAINER_V2"

# =============================================================================
# PASSO 4 — Federated Credential
# =============================================================================
echo ""
info "=== PASSO 4 — Federated Credential ==="
[ -n "$SUB_CLUSTER" ] && az account set --subscription "$SUB_CLUSTER" --output none

FED_NAME="finops-${CLUSTER_NAME}-federated"

EXISTING_FED=$(az identity federated-credential show \
  --name "$FED_NAME" \
  --identity-name "$IDENTITY_NAME" \
  --resource-group "$CLUSTER_RG" \
  --query "name" -o tsv 2>/dev/null || echo "")

if [ -n "$EXISTING_FED" ]; then
  warn "Federated Credential '$FED_NAME' já existe — pulando"
else
  info "Obtendo OIDC Issuer do cluster ..."
  OIDC=$(az aks show \
    --resource-group "$CLUSTER_RG" \
    --name "$CLUSTER_NAME" \
    --query "oidcIssuerProfile.issuerUrl" -o tsv)

  [ -n "$OIDC" ] || err "OIDC Issuer não retornou valor. Verifique se o cluster tem OIDC habilitado."
  info "OIDC: $OIDC"

  az identity federated-credential create \
    --name "$FED_NAME" \
    --identity-name "$IDENTITY_NAME" \
    --resource-group "$CLUSTER_RG" \
    --issuer "$OIDC" \
    --subject "system:serviceaccount:${K8S_NAMESPACE}:finops-workload-identity-sa" \
    --audiences "api://AzureADTokenExchange" \
    --output none

  success "Federated Credential criado"
fi

# =============================================================================
# PASSO 5 — AcrPull para o kubelet identity do cluster
# =============================================================================
echo ""
info "=== PASSO 5 — AcrPull para kubelet identity ==="
[ -n "$SUB_CLUSTER" ] && az account set --subscription "$SUB_CLUSTER" --output none

KUBELET_OID=$(az aks show \
  --resource-group "$CLUSTER_RG" \
  --name "$CLUSTER_NAME" \
  --query "identityProfile.kubeletidentity.objectId" -o tsv)

[ -n "$KUBELET_OID" ] || err "Não foi possível obter o objectId do kubelet identity."
info "Kubelet OID: $KUBELET_OID"

[ -n "$SUB_CLUSTER" ] && az account set --subscription "$SUB_PIPELINE" --output none

SCOPE_ACR="/subscriptions/${SUB_PIPELINE}/resourceGroups/${ACR_RG}/providers/Microsoft.ContainerRegistry/registries/${ACR_NAME}"

assign_role "$SCOPE_ACR" "$ROLE_ACR_PULL" "$KUBELET_OID" "$SUB_PIPELINE" \
  "AcrPull em $ACR_NAME para kubelet"

# =============================================================================
# PASSO 6 — Conectar no cluster
# =============================================================================
echo ""
info "=== PASSO 6 — Conectar no cluster $CLUSTER_NAME ==="
[ -n "$SUB_CLUSTER" ] && az account set --subscription "$SUB_CLUSTER" --output none

az aks get-credentials \
  --resource-group "$CLUSTER_RG" \
  --name "$CLUSTER_NAME" \
  --overwrite-existing \
  --output none

CURRENT=$(kubectl config current-context)
success "Conectado: $CURRENT"

# =============================================================================
# PASSO 7 — Namespace e ServiceAccount com anotação correta
# =============================================================================
echo ""
info "=== PASSO 7 — Namespace e ServiceAccount ==="

kubectl create namespace "$K8S_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
success "Namespace $K8S_NAMESPACE OK"

kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: finops-workload-identity-sa
  namespace: ${K8S_NAMESPACE}
  annotations:
    azure.workload.identity/client-id: "${CLIENT_ID}"
EOF
success "ServiceAccount criado/atualizado com CLIENT_ID: $CLIENT_ID"

# =============================================================================
# PASSO 8 — Aplicar CronJob (substitui CLIENT_ID e CLUSTER_NAME)
# =============================================================================
echo ""
info "=== PASSO 8 — Aplicar CronJob ==="

HML_CLIENT_ID="5eed8b89-7707-44cf-85e3-a6f86866a25f"

YAML_TEMP=$(mktemp /tmp/cronjob-aks-${CLUSTER_NAME}-XXXXXX.yaml)

sed \
  -e "s|azure.workload.identity/client-id: \"${HML_CLIENT_ID}\"|azure.workload.identity/client-id: \"${CLIENT_ID}\"|g" \
  -e "s|CLUSTER_NAME: \"aks-hml-newkmm\"|CLUSTER_NAME: \"${CLUSTER_NAME}\"|g" \
  k8s/cronjob-aks.yaml > "$YAML_TEMP"

info "Recursos que serão criados/atualizados:"
grep "^kind:\|^  name:" "$YAML_TEMP" | sed 's/^/  /'
echo ""

kubectl apply -f "$YAML_TEMP"
rm -f "$YAML_TEMP"
success "CronJob aplicado"

# =============================================================================
# PASSO 9 — Teste manual
# =============================================================================
echo ""
info "=== PASSO 9 — Teste manual ==="

JOB_NAME="finops-aks-test-$(date +%s)"
info "Criando job de teste: $JOB_NAME ..."

kubectl create job \
  --from=cronjob/finops-aks-allocation \
  "$JOB_NAME" \
  -n "$K8S_NAMESPACE"

success "Job criado"
echo ""
info "Aguardando pod iniciar (25s) ..."
sleep 25

POD=$(kubectl get pods -n "$K8S_NAMESPACE" \
  -l "job-name=$JOB_NAME" \
  --no-headers \
  -o custom-columns=":metadata.name" 2>/dev/null | head -1 || echo "")

if [ -n "$POD" ]; then
  POD_STATUS=$(kubectl get pod "$POD" -n "$K8S_NAMESPACE" \
    --no-headers -o custom-columns=":status.phase" 2>/dev/null || echo "Unknown")
  success "Pod: $POD (status: $POD_STATUS)"
  echo ""
  info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  info "LOGS (Ctrl+C para sair — o job continua rodando)"
  info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""
  kubectl logs -f "$POD" -n "$K8S_NAMESPACE" || true
else
  warn "Pod ainda não disponível. Verifique manualmente:"
  echo ""
  echo "  kubectl get pods -n $K8S_NAMESPACE -l job-name=$JOB_NAME"
  echo "  kubectl logs -f -l job-name=$JOB_NAME -n $K8S_NAMESPACE"
  echo "  kubectl describe job $JOB_NAME -n $K8S_NAMESPACE"
fi

# =============================================================================
# RESUMO FINAL
# =============================================================================
echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}  Deploy $CLUSTER_NAME concluído!${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""
echo "  Cluster  : $CLUSTER_NAME"
echo "  Identity : $IDENTITY_NAME"
echo "  CLIENT_ID: $CLIENT_ID"
echo ""
if [ ${#PENDING_PERMS[@]} -gt 0 ]; then
  echo -e "${YELLOW}  ⚠ Permissões pendentes (requerem Owner/UAA na subscription):${NC}"
  for p in "${PENDING_PERMS[@]}"; do
    echo "    - $p"
  done
  echo ""
  echo "  Compartilhe os comandos 'az rest' exibidos acima com quem tem Owner na sub."
  echo "  Depois de atribuídas, o CronJob já funciona na próxima execução."
else
  echo "  Atualize o CLAUDE.md marcando este cluster como configurado."
fi
echo ""
echo "  Comandos úteis:"
echo ""
echo "  kubectl get cronjob finops-aks-allocation -n $K8S_NAMESPACE"
echo "  kubectl create job --from=cronjob/finops-aks-allocation finops-aks-manual-\$(date +%s) -n $K8S_NAMESPACE"
echo "  kubectl get jobs -n $K8S_NAMESPACE"
echo ""
# Restaura subscription padrão do projeto ao terminar
[ -n "$SUB_CLUSTER" ] && az account set --subscription "$SUB_PIPELINE" --output none

echo "  az storage blob list \\"
echo "    --account-name $STORAGE_PIPELINE \\"
echo "    --container-name $CONTAINER_V2 \\"
echo "    --prefix gold/aks_allocation_share_opencost/ \\"
echo "    --auth-mode login --output table"
