# Guia de Manutenção

Este guia explica como fazer as mudanças mais comuns no pipeline sem quebrar nada.

---

## Adicionar uma nova subscription Azure

**Quando:** a empresa adquire uma nova subscription e os custos precisam entrar no rateio.

**Impacto:** todos os recursos da nova subscription passam a ser processados e alocados.

**Passos:**

1. **Adicionar o GUID em `BILLING_SUBSCRIPTION_IDS`** no ConfigMap Kubernetes:
   ```bash
   kubectl edit configmap finops-config -n finops
   # Adicionar o novo GUID separado por vírgula
   # BILLING_SUBSCRIPTION_IDS=guid1,guid2,novo-guid
   ```

2. **Permissão da Managed Identity na nova subscription:**
   ```bash
   OBJECT_ID=$(az identity show --name id-mg-kmm-finops-aks-prd-newkmm-eus-prd \
     --resource-group RG-US-PRD-KMM --query principalId -o tsv)

   az role assignment create \
     --assignee $OBJECT_ID \
     --role "Reader" \
     --scope "/subscriptions/<NOVO-GUID>"
   ```

3. **Reiniciar o pipeline:**
   ```bash
   kubectl rollout restart cronjob/finops-pipeline -n finops
   kubectl rollout restart cronjob/finops-sync-rg-tags -n finops
   ```

4. **Rodar retrocarga** para processar os dias anteriores do mês:
   ```bash
   python sh/retro_fix.py --start <primeiro-dia-do-mes> --end <ontem>
   ```

5. **Taguear os Resource Groups** da nova subscription com `FINOPS-CLIENTE` para evitar que tudo caia em WEIGHT_PROPORTIONAL.

---

## Adicionar um novo cluster AKS compartilhado

**Quando:** um novo cluster é criado e precisa ter seus custos rateados por namespace.

**Pré-requisito obrigatório:** instalar o OpenCost no cluster antes de qualquer coisa.

```bash
# Instalar OpenCost
helm repo add opencost https://opencost.github.io/opencost-helm-chart
helm install opencost opencost/opencost \
  --namespace opencost \
  --create-namespace \
  --set opencost.exporter.defaultClusterId=<NOME-DO-CLUSTER>

# Verificar que está rodando
kubectl get pods -n opencost
# Testar endpoint
kubectl port-forward svc/opencost 9003:9003 -n opencost
curl "http://localhost:9003/allocation?window=1d"
```

**Passos após OpenCost instalado:**

1. **Criar Managed Identity:**
   ```bash
   az identity create \
     --name id-finops-<nome-cluster> \
     --resource-group <RG-DO-CLUSTER> \
     --location eastus

   CLIENT_ID=$(az identity show --name id-finops-<nome-cluster> \
     --resource-group <RG> --query clientId -o tsv)
   OBJECT_ID=$(az identity show --name id-finops-<nome-cluster> \
     --resource-group <RG> --query principalId -o tsv)

   # Permissão de leitura no FOCUS
   az role assignment create --assignee $OBJECT_ID \
     --role "Storage Blob Data Reader" \
     --scope "/subscriptions/0535039d-.../storageAccounts/azurecostexportfocus10"

   # Permissão de escrita no pipeline
   az role assignment create --assignee $OBJECT_ID \
     --role "Storage Blob Data Contributor" \
     --scope "/subscriptions/52d4423b-.../storageAccounts/stpslkmmfinopseusprd/blobServices/default/containers/finops-v2"

   # Federated Credential
   OIDC=$(az aks show --resource-group <RG> --name <CLUSTER> \
     --query "oidcIssuerProfile.issuerUrl" -o tsv)
   az identity federated-credential create \
     --name finops-aks-federated \
     --identity-name id-finops-<nome-cluster> \
     --resource-group <RG> \
     --issuer $OIDC \
     --subject "system:serviceaccount:finops:finops-workload-identity-sa" \
     --audiences "api://AzureADTokenExchange"
   ```

2. **Adicionar em `cluster_config.csv`:**
   ```csv
   ClusterName,ResourceGroup,Type,DedicatedCliente,OpenCostURL
   novo-cluster,RG-NOVO,shared,,http://<ip-opencost>:9003
   ```

3. **Upload do arquivo atualizado:**
   ```bash
   az storage blob upload \
     --account-name stpslkmmfinopseusprd \
     --container-name finops-v2 \
     --name "config/reference/cluster_config.csv" \
     --file "config/reference/cluster_config.csv" \
     --auth-mode login
   ```

4. **Aplicar o CronJob do s03b no novo cluster:**
   ```bash
   az aks get-credentials --resource-group <RG> --name <CLUSTER>
   kubectl create namespace finops
   kubectl apply -f k8s/serviceaccount.yaml -n finops
   kubectl apply -f k8s/cronjob-aks.yaml -n finops
   ```

5. Atualizar `CLIENT_ID` no `k8s/cronjob-aks.yaml` com o `clientId` da nova identity.

---

## Remover ou substituir um cluster AKS

**Quando:** um cluster é descomissionado ou substituído por outro.

1. **Remover o CronJob do cluster antigo:**
   ```bash
   az aks get-credentials --resource-group <RG-ANTIGO> --name <CLUSTER-ANTIGO>
   kubectl delete cronjob finops-pipeline finops-sync-rg-tags -n finops
   ```

2. **Remover da `cluster_config.csv`** e fazer upload.

3. **Manter os dados históricos** — os arquivos em `gold/aks_allocation_share_opencost/` do cluster antigo não precisam ser apagados.

---

## Adicionar um novo cliente

**Quando:** um novo cliente começa a usar a infraestrutura KMM.

**Passos:**

1. **Criar os Resource Groups** do cliente na Azure com a tag:
   - Tag: `FINOPS-CLIENTE` = `NOME_DO_CLIENTE` (usar maiúsculas, sem espaços)

2. **Adicionar aliases de namespace em `clientes_alias.csv`**:
   ```csv
   Alias,Cliente
   nome-do-cliente,NOME_DO_CLIENTE
   nome-ns-api,NOME_DO_CLIENTE
   nome-ns-worker,NOME_DO_CLIENTE
   ```

3. **Upload do arquivo atualizado:**
   ```bash
   az storage blob upload \
     --account-name stpslkmmfinopseusprd \
     --container-name finops-v2 \
     --name "config/reference/clientes_alias.csv" \
     --file "config/reference/clientes_alias.csv" \
     --auth-mode login
   ```

4. O pipeline detecta o novo cliente automaticamente na próxima execução. Não é necessário reiniciar nada.

---

## Remover um cliente

**Quando:** um cliente encerra os serviços.

1. **Remover os aliases** de `clientes_alias.csv` e fazer upload.
2. **Manter os dados históricos** — não apagar registros do parquet consolidado.
3. Os recursos do cliente no Azure devem ser descomissionados ou retagueados pela operação.

---

## Adicionar um novo vault de backup

**Quando:** um novo Recovery Services Vault é criado e precisa ter seus custos rateados.

1. **Adicionar em `config/reference/backup_vaults.csv`:**
   ```csv
   VaultName,ResourceGroup,SubscriptionId,ServiceKey
   novo-vault,rg-backup,52d4423b-...,VAULT:novo-vault
   ```

2. **Upload do arquivo:**
   ```bash
   az storage blob upload \
     --account-name stpslkmmfinopseusprd \
     --container-name finops-v2 \
     --name "config/reference/backup_vaults.csv" \
     --file "config/reference/backup_vaults.csv" \
     --auth-mode login
   ```

3. O pipeline processa automaticamente na próxima execução.

---

## Melhorar a qualidade do rateio (reduzir WEIGHT_PROPORTIONAL)

**Quando:** o percentual de WEIGHT_PROPORTIONAL está alto e os clientes questionam o rateio.

**Como identificar o que taguear:**

Baixar `gold/fact_allocated_cost/dt={data}/unallocated_cost_{data}.parquet` e ordenar por `Cost` decrescente — os recursos no topo têm maior impacto.

Ou usar a view `vw_recursos_sem_tag` descrita em [POWER_BI.md](./POWER_BI.md).

**Como corrigir:**

No portal Azure, acessar o Resource Group e adicionar a tag:
- Tag: `FINOPS-CLIENTE`
- Valor: nome do cliente em maiúsculas (ex: `NORCOAST`, `MARONI`, `LTL`, `DADOS`)
- Para recursos compartilhados explícitos: valor `COMPARTILHADO`

O `s00` sincroniza as tags diariamente às 04:00 UTC. A partir da próxima execução do pipeline, o recurso sai do WEIGHT_PROPORTIONAL.

**Para retroagir a correção:**
```bash
python sh/retro_fix.py --start <data-inicio> --end <data-fim>
```

---

## Reprocessar dias passados (retrocarga)

**Quando:** houve um bug corrigido, billing lag prolongado, ou mudança de tags que precisa ser aplicada retroativamente.

```bash
# Reprocessar dias específicos
python sh/retro_fix.py --start 2026-05-01 --end 2026-05-07

# Reconstruir o consolidado Power BI após a retrocarga
kubectl delete job finops-fix-consolidated -n finops --ignore-not-found=true
kubectl apply -f k8s/job-fix-consolidated.yaml
kubectl wait --for=condition=complete --timeout=600s job/finops-fix-consolidated -n finops
kubectl logs job/finops-fix-consolidated -n finops | grep -E "s07|concluida|ERROR"
```

> ⚠️ Não rodar retrocarga de mais de 30 dias de uma vez — pode causar timeout ou OOM. Dividir em blocos de 7 dias.

---

## Verificar saúde do pipeline

```bash
# Últimas execuções
kubectl get jobs -n finops --sort-by=.metadata.creationTimestamp | tail -10

# Logs do último job
LAST_JOB=$(kubectl get jobs -n finops --sort-by=.metadata.creationTimestamp \
  -o jsonpath='{.items[-1].metadata.name}')
kubectl logs job/$LAST_JOB -n finops | tail -50

# Verificar manifest no Blob
az storage blob download \
  --account-name stpslkmmfinopseusprd \
  --container-name finops-v2 \
  --name "gold/fact_allocated_cost_consolidated/manifest.csv" \
  --file /tmp/manifest.csv \
  --auth-mode login
cat /tmp/manifest.csv
```

**Sinais de problema:**
- `Status=ERROR` no manifest
- `TotalAllocated=0` para alguma data
- `TotalAllocated` muito diferente de `TotalCost` (tolerância < R$ 1,00)
- Job Kubernetes com status `Failed`

---

## Atualizar o tema ou layout do Power BI

O arquivo de tema `nstech_powerbi_theme.json` pode ser editado e reaplicado a qualquer momento:

**Power BI Desktop → Exibição → Temas → Procurar temas → selecionar o arquivo**

Não requer nenhuma alteração no pipeline ou no parquet.

---

## FAQ

**P: O pipeline rodou mas o Power BI não atualizou.**
R: Fazer refresh manual no Power BI Desktop: **Início → Atualizar**. Se no Service, verificar se o refresh agendado está configurado e se as credenciais do dataset estão válidas.

**P: Um cliente reclamou que o custo dele subiu muito.**
R: Verificar no Power BI a coluna `AllocationMethod`. Se a maioria passou de `DEDICATED_TAG` para `WEIGHT_PROPORTIONAL`, algum RG perdeu a tag. Verificar no portal Azure e reaplicar a tag.

**P: Preciso adicionar um novo tipo de rateio (ex: por storage).**
R: Criar `s03e_storage.py` seguindo o padrão de `s03c_sql_pool.py`. Registrar o `ServiceType` novo em `s02_classify.py`. Adicionar leitura do novo share em `s05_consolidate.py`. Incluir na ordem em `run_pipeline.py`. Documentar o novo `AllocationMethod` neste guia.

**P: O OpenCost de um cluster está fora do ar.**
R: O `s03b` tem fallback — se a consulta ao OpenCost falhar, os recursos do cluster ficam em `WEIGHT_PROPORTIONAL` para o dia. O pipeline não aborta. Verificar o pod do OpenCost: `kubectl get pods -n opencost -l app=opencost`.

**P: Como saber qual versão da imagem está rodando?**
R: `kubectl describe cronjob finops-pipeline -n finops | grep Image`
