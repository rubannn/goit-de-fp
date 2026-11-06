"""Run the batch pipeline through Airflow, or exercise all jobs locally in an isolated root."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import time
from urllib.parse import quote
import requests
from project_config import DATA_ROOT

PROJECT = Path("/opt/airflow/project")
ARTIFACTS = PROJECT / "artifacts/batch"


def local_run():
    environment = dict(os.environ, DATA_ROOT=str(DATA_ROOT / "_local_batch"))
    for script in ("batch/landing_to_bronze.py", "batch/bronze_to_silver.py", "batch/silver_to_gold.py", "scripts/check_batch.py"):
        arguments = ["spark-submit", str(PROJECT / script)]
        if "landing_to_bronze" in script:
            arguments += ["--refresh"]
        print(f"LOCAL {script} -> {environment['DATA_ROOT']}", flush=True)
        with (ARTIFACTS / f"local_{Path(script).stem}.log").open("w") as output:
            subprocess.run(arguments, env=environment, stdout=output, stderr=subprocess.STDOUT, check=True)
    print("LOCAL batch pipeline and independent verification: PASS", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--dag-id", default="olympic_batch")
    parser.add_argument("--source-url")
    parser.add_argument("--expect-failure", action="store_true")
    args = parser.parse_args()
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    if args.local:
        local_run()
        return
    session = requests.Session()
    password = (Path(os.environ["AIRFLOW_HOME"]) / "standalone_admin_password.txt").read_text().strip()
    session.auth = ("admin", password)
    base = "http://localhost:8080/api/v1/dags/" + quote(args.dag_id, safe="")
    deadline = time.monotonic() + 420
    while True:
        response = session.get(base, timeout=30)
        if response.status_code == 200:
            break
        if response.status_code != 404 or time.monotonic() > deadline:
            response.raise_for_status()
        time.sleep(3)
    response = session.patch(base, json={"is_paused": False}, timeout=30)
    response.raise_for_status()
    run_id = "fp_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    conf = {"source_url": args.source_url} if args.source_url else {}
    response = session.post(base + "/dagRuns", json={"dag_run_id": run_id, "conf": conf}, timeout=30)
    response.raise_for_status()
    endpoint = base + "/dagRuns/" + quote(run_id, safe="")
    print(f"AIRFLOW triggered {args.dag_id}: {run_id}", flush=True)
    deadline = time.monotonic() + 3600
    previous = None
    while True:
        response = session.get(endpoint, timeout=30)
        response.raise_for_status()
        run = response.json()
        response = session.get(endpoint + "/taskInstances", timeout=30)
        response.raise_for_status()
        tasks = response.json()["task_instances"]
        states = {task["task_id"]: task["state"] for task in tasks}
        progress = {"dag_state": run["state"], "tasks": states}
        if progress != previous:
            print(json.dumps(progress), flush=True)
            previous = progress
        if run["state"] in ("success", "failed"):
            break
        if time.monotonic() > deadline:
            raise TimeoutError(f"DAG run still active: {run_id}")
        time.sleep(5)
    for task in tasks:
        if task["state"] not in ("success", "failed"):
            continue
        path = endpoint + "/taskInstances/" + quote(task["task_id"], safe="") + "/logs/1"
        response = session.get(path, params={"full_content": "true"}, headers={"Accept": "application/json"}, timeout=30)
        response.raise_for_status()
        (ARTIFACTS / f"{run_id}_{task['task_id']}.log").write_text(response.json()["content"], encoding="utf-8")
    summary = {"dag_id": args.dag_id, "run_id": run_id, "state": run["state"], "tasks": states,
               "start_date": run["start_date"], "end_date": run["end_date"], "conf": conf}
    (ARTIFACTS / f"{run_id}.json").write_text(json.dumps(summary, indent=2))
    (ARTIFACTS / "last_dag_run.json").write_text(json.dumps(summary, indent=2))
    print("AIRFLOW COMPLETE " + json.dumps(summary), flush=True)
    expected = "failed" if args.expect_failure else "success"
    if run["state"] != expected:
        raise RuntimeError(f"DAG state {run['state']}; expected {expected}")
    if args.expect_failure and args.dag_id == "olympic_batch":
        assert states == {"landing_to_bronze": "failed", "bronze_to_silver": "upstream_failed", "silver_to_gold": "upstream_failed"}


if __name__ == "__main__":
    main()
