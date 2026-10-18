import os
import re
import subprocess
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator


def run_pipeline(**context):
    months = context["params"]["months"]
    if not months or any(not re.fullmatch(r"20\d{2}-(0[1-9]|1[0-2])", m) for m in months):
        raise ValueError("Invalid monthly partition")
    python = os.environ.get("FLIGHTLEDGER_PYTHON", "python")
    root = os.environ.get("FLIGHTLEDGER_ROOT", "/opt/flightledger")
    subprocess.run([python, "-m", "flightledger.cli", "--root", root, "sync", "--months", *months, "--refresh"], check=True)


def audit_publication():
    python = os.environ.get("FLIGHTLEDGER_PYTHON", "python")
    root = os.environ.get("FLIGHTLEDGER_ROOT", "/opt/flightledger")
    subprocess.run([python, "-m", "flightledger.cli", "--root", root, "audit"], check=True)


with DAG(
    "flightledger_revisions", start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="0 8 * * 1", catchup=False, max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=3)},
    params={"months": Param(["2025-01", "2025-02", "2025-03"], type="array", minItems=1, items={"type": "string", "pattern": r"^20\d{2}-(0[1-9]|1[0-2])$"})},
    tags=["anac", "lakehouse", "revisions"],
) as dag:
    load = PythonOperator(task_id="sync_monthly_snapshots", python_callable=run_pipeline, execution_timeout=timedelta(minutes=45))
    verify = PythonOperator(task_id="verify_published_checksum", python_callable=audit_publication)
    load >> verify
