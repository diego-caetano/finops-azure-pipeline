#!/usr/bin/env python3
"""Retrocarga pipeline FinOps — 2026-05-01 a 2026-05-15"""

import io
import json
import re
import subprocess
import sys

# Force UTF-8 output to avoid CP1252 issues on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

DATES = [
    "2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05",
    "2026-05-06", "2026-05-07", "2026-05-08", "2026-05-09", "2026-05-10",
    "2026-05-11", "2026-05-12", "2026-05-13", "2026-05-14", "2026-05-15",
]


def run(cmd, **kwargs):
    return subprocess.run(cmd, capture_output=True, encoding="utf-8", errors="replace", **kwargs)


def banner(msg):
    print(f"\n{'='*44}\n{msg}\n{'='*44}", flush=True)


# Carrega template do CronJob uma vez
r = run(["kubectl", "get", "cronjob", "finops-pipeline", "-n", "finops", "-o", "json"], check=True)
cronjob = json.loads(r.stdout)

pass_count = 0
fail_count = 0

for date in DATES:
    suffix = date.replace("-", "")
    job_name = f"finops-retro-{suffix}"

    banner(f">> {date} -> {job_name}")

    # Remove job anterior se existir
    run(["kubectl", "delete", "job", job_name, "-n", "finops", "--ignore-not-found=true"])

    # Deep-copy do spec e injeção de PIPELINE_DATE
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
        print(f"ERRO {date} ao criar job:\n{r.stderr}", flush=True)
        fail_count += 1
        continue

    print(f"   job criado, aguardando conclusão (timeout 10min)...", flush=True)

    r = run(["kubectl", "wait", "--for=condition=complete", "--timeout=600s",
             f"job/{job_name}", "-n", "finops"])

    logs = run(["kubectl", "logs", f"job/{job_name}", "-n", "finops"]).stdout

    if r.returncode == 0:
        summary = ""
        for line in logs.splitlines():
            if "TotalAllocated" in line:
                m = re.search(r"TotalFactCost=([\d.]+).*TotalAllocated=([\d.]+)\s+\(([\d.]+)%\)", line)
                if m:
                    summary = f"FactCost={float(m.group(1)):.2f}  Allocated={float(m.group(2)):.2f}  ({m.group(3)}%)"
                break
        print(f"OK {date}  {summary}", flush=True)
        pass_count += 1
    else:
        print(f"FALHOU {date} -- ultimas linhas:", flush=True)
        for line in logs.splitlines()[-15:]:
            if line.strip():
                print(f"   {line}", flush=True)
        fail_count += 1

banner(f"Retrocarga concluída: {pass_count} OK  |  {fail_count} FALHOU")
sys.exit(0 if fail_count == 0 else 1)
