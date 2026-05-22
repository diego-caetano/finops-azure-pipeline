#!/usr/bin/env python3
"""Retrocarga corretiva — 2026-05-15 a 2026-05-20

Re-executa o pipeline completo (s01→s07) para as datas com dados incompletos
no FOCUS Report (Azure ainda não havia liquidado os charges quando processados
originalmente). Usa o FOCUS atualizado em 2026-05-21 que já tem dados mais
completos para essas datas.
"""

import io
import json
import re
import subprocess
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

DATES = [
    "2026-05-15",
    "2026-05-16",
    "2026-05-17",
    "2026-05-18",
    "2026-05-19",
    "2026-05-20",
]


def run(cmd, **kwargs):
    return subprocess.run(cmd, capture_output=True, encoding="utf-8", errors="replace", **kwargs)


def banner(msg):
    print(f"\n{'='*50}\n{msg}\n{'='*50}", flush=True)


r = run(["kubectl", "get", "cronjob", "finops-pipeline", "-n", "finops", "-o", "json"], check=True)
cronjob = json.loads(r.stdout)

pass_count = 0
fail_count = 0

for date in DATES:
    suffix = date.replace("-", "")
    job_name = f"finops-fix-{suffix}"

    banner(f">> {date} -> {job_name}")

    run(["kubectl", "delete", "job", job_name, "-n", "finops", "--ignore-not-found=true"])

    job_spec = json.loads(json.dumps(cronjob["spec"]["jobTemplate"]["spec"]))
    for c in job_spec["template"]["spec"]["containers"]:
        if c["name"] == "finops-pipeline":
            env = [e for e in c.get("env", []) if e["name"] != "PIPELINE_DATE"]
            env.append({"name": "PIPELINE_DATE", "value": date})
            c["env"] = env

    job_manifest = json.dumps({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name, "namespace": "finops"},
        "spec": job_spec,
    })

    r = run(["kubectl", "apply", "-f", "-"], input=job_manifest)
    if r.returncode != 0:
        print(f"ERRO ao criar job para {date}:\n{r.stderr}", flush=True)
        fail_count += 1
        continue

    print(f"   job criado, aguardando conclusão (timeout 15min)...", flush=True)

    r = run(["kubectl", "wait", "--for=condition=complete", "--timeout=900s",
             f"job/{job_name}", "-n", "finops"])

    logs = run(["kubectl", "logs", f"job/{job_name}", "-n", "finops"]).stdout

    if r.returncode == 0:
        summary = ""
        for line in logs.splitlines():
            if "TotalFactCost" in line and "TotalAllocated" in line:
                m = re.search(
                    r"TotalFactCost=([\d.]+).*?TotalAllocated=([\d.]+)\s+\(([\d.]+)%\)",
                    line
                )
                if m:
                    summary = (
                        f"FactCost={float(m.group(1)):,.2f}  "
                        f"Allocated={float(m.group(2)):,.2f}  "
                        f"({m.group(3)}%)"
                    )
                break
        print(f"OK  {date}  {summary}", flush=True)
        pass_count += 1
    else:
        print(f"FALHOU {date} — últimas linhas:", flush=True)
        for line in logs.splitlines()[-15:]:
            if line.strip():
                print(f"   {line}", flush=True)
        fail_count += 1

banner(f"Retrocarga corretiva concluída: {pass_count} OK  |  {fail_count} FALHOU")
sys.exit(0 if fail_count == 0 else 1)
