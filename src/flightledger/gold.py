import json
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

import duckdb
from deltalake import DeltaTable

from .storage import atomic_json, read_json, sha256


def publish(root: Path, index: dict, changed_months: list[str], full_refresh: bool = False, fail_before_publish: bool = False, model_hash: str = "") -> dict:
    previous = read_json(root / "data" / "published.json", {})
    snapshot_id = uuid.uuid4().hex
    target = root / "data" / "gold" / snapshot_id
    target.mkdir(parents=True)
    warehouse = target / "warehouse.duckdb"
    if previous and not full_refresh:
        shutil.copyfile(root / previous["warehouse"], warehouse)
    delta = DeltaTable(root / "data" / "silver" / "flights")
    delta_version = delta.version()
    arrow = delta.to_pyarrow_table()
    with duckdb.connect(str(warehouse)) as conn:
        conn.execute("SET threads=2")
        conn.execute("SET TimeZone='UTC'")
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.register("incoming", arrow)
        conn.execute("CREATE OR REPLACE TABLE raw.flights AS SELECT * FROM incoming")
        conn.execute("CREATE OR REPLACE TABLE raw.airports AS SELECT * FROM read_csv(?, header=true, all_varchar=true)", [str(root / index["airports"]["path"])])
        conn.execute("CHECKPOINT")
    env = dict(os.environ, FLIGHTLEDGER_WAREHOUSE=str(warehouse), DBT_SEND_ANONYMOUS_USAGE_STATS="false",
               DBT_LOG_PATH=str(target / "logs"))
    executable = str(Path(sys.executable).with_name("dbt.exe" if os.name == "nt" else "dbt"))
    command = [executable, "build", "--project-dir", str(root / "dbt"), "--profiles-dir", str(root / "dbt"),
               "--target-path", str(target / "dbt"), "--vars", json.dumps({"changed_months": changed_months})]
    if full_refresh:
        command.append("--full-refresh")
    log = subprocess.run(command, env=env, capture_output=True, text=True)
    (target / "dbt_build.log").write_text(log.stdout + log.stderr, encoding="utf-8")
    if log.returncode:
        raise RuntimeError(f"dbt build failed; see {target / 'dbt_build.log'}\n{log.stdout[-2500:]}")
    if fail_before_publish:
        raise RuntimeError("Injected failure before publication")
    dbt_results = read_json(target / "dbt" / "run_results.json")
    quality = [read_json(root / "data" / "quality" / f"{month}.json") for month in sorted(index["months"])]
    atomic_json(target / "quality.json", quality)
    metadata = {"snapshot_id": snapshot_id, "warehouse": warehouse.relative_to(root).as_posix(),
                "delta_version": delta_version, "source_hashes": {m: v["sha256"] for m, v in index["months"].items()},
                "airports_hash": index["airports"]["sha256"], "changed_months": changed_months,
                "dbt_tests": sum(r["unique_id"].startswith("test.") for r in dbt_results["results"]),
                "warehouse_sha256": sha256(warehouse), "model_hash": model_hash}
    metadata["quality"] = (target / "quality.json").relative_to(root).as_posix()
    atomic_json(target / "manifest.json", metadata)
    atomic_json(root / "data" / "published.json", metadata)
    return metadata


def export_s3(root: Path, bucket: str, prefix: str = "flightledger", endpoint: str | None = None, client=None) -> dict:
    import boto3
    published = read_json(root / "data" / "published.json")
    if not published:
        raise ValueError("No published snapshot")
    client = client or boto3.client("s3", endpoint_url=endpoint)
    directory = root / "data" / "gold" / published["snapshot_id"] / "export"
    directory.mkdir(exist_ok=True)
    with duckdb.connect(str(root / published["warehouse"]), read_only=True) as conn:
        for model in ["fct_flight", "dim_airport", "fct_daily_operations", "mart_route_reliability"]:
            destination = directory / f"{model}.parquet"
            conn.sql(f"SELECT * FROM analytics.{model}").write_parquet(str(destination), compression="zstd", overwrite=True)
    objects = []
    for path in sorted(directory.glob("*.parquet")):
        key = f"{prefix.strip('/')}/snapshots/{published['snapshot_id']}/{path.name}"
        client.upload_file(str(path), bucket, key)
        objects.append({"key": key, "sha256": sha256(path), "bytes": path.stat().st_size})
    manifest = {"snapshot_id": published["snapshot_id"], "objects": objects}
    key = f"{prefix.strip('/')}/snapshots/{published['snapshot_id']}/manifest.json"
    client.put_object(Bucket=bucket, Key=key, Body=json.dumps(manifest).encode(), ContentType="application/json")
    return manifest
