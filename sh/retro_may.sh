#!/bin/bash
# Retrocarga pipeline FinOps — 2026-05-01 a 2026-05-15
# Usa dry-run + python para injetar PIPELINE_DATE no template do CronJob.

set -euo pipefail

DATES=(
  "2026-05-01" "2026-05-02" "2026-05-03" "2026-05-04" "2026-05-05"
  "2026-05-06" "2026-05-07" "2026-05-08" "2026-05-09" "2026-05-10"
  "2026-05-11" "2026-05-12" "2026-05-13" "2026-05-14" "2026-05-15"
)

PASS=0
FAIL=0

# Pré-carrega o spec do CronJob uma única vez
CRONJOB_JSON=$(kubectl get cronjob finops-pipeline -n finops -o json)

for DATE in "${DATES[@]}"; do
  SUFFIX=$(echo "$DATE" | tr -d '-')
  JOB="finops-retro-$SUFFIX"

  echo ""
  echo "========================================"
  echo "▶  $DATE  →  job: $JOB"
  echo "========================================"

  # Remove job anterior com mesmo nome se existir
  kubectl delete job "$JOB" -n finops --ignore-not-found=true >/dev/null 2>&1

  # Monta manifesto do Job injetando PIPELINE_DATE
  echo "$CRONJOB_JSON" | python3 - <<PYEOF "$JOB" "$DATE" | kubectl apply -f - >/dev/null
import sys, json

job_name = sys.argv[1]
pipeline_date = sys.argv[2]
d = json.loads(sys.stdin.read())

job_spec = d["spec"]["jobTemplate"]["spec"]
containers = job_spec["template"]["spec"]["containers"]
for c in containers:
    if c["name"] == "finops-pipeline":
        env = [e for e in c.get("env", []) if e["name"] != "PIPELINE_DATE"]
        env.append({"name": "PIPELINE_DATE", "value": pipeline_date})
        c["env"] = env

job = {
    "apiVersion": "batch/v1",
    "kind": "Job",
    "metadata": {"name": job_name, "namespace": "finops"},
    "spec": job_spec,
}
print(json.dumps(job))
PYEOF

  # Aguarda até 10 min
  if kubectl wait --for=condition=complete --timeout=600s job/"$JOB" -n finops 2>/dev/null; then
    TOTAL=$(kubectl logs job/"$JOB" -n finops 2>/dev/null | grep "TotalAllocated" | grep -oP 'TotalAllocated=[\d.]+' | head -1)
    echo "✅ $DATE  OK  $TOTAL"
    PASS=$((PASS + 1))
  else
    echo "❌ $DATE  FALHOU — últimas linhas:"
    kubectl logs job/"$JOB" -n finops 2>/dev/null | tail -10
    FAIL=$((FAIL + 1))
  fi
done

echo ""
echo "========================================"
echo "Retrocarga concluída: $PASS OK  |  $FAIL FALHOU"
echo "========================================"
