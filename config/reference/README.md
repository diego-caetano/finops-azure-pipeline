# config/reference/

Arquivos de configuração do pipeline FinOps.
Estes arquivos devem ser enviados para o Blob Storage em:
  container: finops
  path:      config/reference/

## Arquivos

| Arquivo                    | Descrição                                      |
|----------------------------|------------------------------------------------|
| cluster_config.csv         | Mapeamento de clusters AKS (tipo, URL, cliente)|
| clientes_alias.csv         | Alias de namespaces → nome do cliente          |
| aks_system_namespaces.csv  | Namespaces de sistema a serem redistribuídos   |
| overrides_rg.csv           | Overrides por Resource Group                   |

## Como enviar para o Blob Storage

```bash
az storage blob upload \
  --account-name stpslkmmfinopseusprd \
  --container-name finops \
  --name config/reference/cluster_config.csv \
  --file config/reference/cluster_config.csv \
  --auth-mode login
```
