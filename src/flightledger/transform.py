import csv
import os
import tempfile
from pathlib import Path

import pyarrow.dataset as ds
from deltalake import DeltaTable, write_deltalake
from pyspark.sql import SparkSession, functions as F

from .storage import atomic_json, sha256

FIELDS = {
    "Sigla ICAO Empresa Aérea": "airline", "Número Voo": "flight_number",
    "Código DI": "di", "Código Tipo Linha": "line_type",
    "Sigla ICAO Aeroporto Origem": "origin", "Sigla ICAO Aeroporto Destino": "destination",
    "Partida Prevista": "scheduled_departure", "Partida Real": "actual_departure",
    "Chegada Prevista": "scheduled_arrival", "Chegada Real": "actual_arrival",
    "Situação Voo": "status", "Referência": "reference_date", "Justificativa": "justification",
}
TIMESTAMPS = ["scheduled_departure", "actual_departure", "scheduled_arrival", "actual_arrival"]


def session() -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = (SparkSession.builder.master("local[2]").appName("FlightLedger")
             .config("spark.ui.enabled", "false").config("spark.driver.memory", "1g")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
             .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def normalize(spark: SparkSession, path: Path, period: str, digest: str):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        header = next(csv.reader(stream, delimiter=";"))
    missing = set(FIELDS) - set(header)
    if missing:
        raise ValueError(f"Missing source columns: {sorted(missing)}")
    raw = spark.read.option("header", True).option("sep", ";").option("encoding", "UTF-8").option("mode", "FAILFAST").csv(str(path))
    selected = raw.select([F.trim(F.col(src)).alias(dst) for src, dst in FIELDS.items()])
    frame = selected
    for col in TIMESTAMPS:
        parsed = F.to_timestamp(F.col(col), "dd/MM/yyyy HH:mm")
        frame = frame.withColumn(f"{col}_utc", F.to_utc_timestamp(parsed, "America/Sao_Paulo"))
    frame = (frame.withColumn("reference_date_parsed", F.to_date(F.to_timestamp("reference_date", "dd/MM/yyyy HH:mm:ss")))
             .withColumn("status", F.regexp_replace(F.translate(F.upper("status"), "ÃÁÂÀÉÊÍÓÔÕÚÜÇ", "AAAAEEIOOOUUC"), " +", "_")))
    invalid_time = F.lit(False)
    for col in TIMESTAMPS:
        invalid_time = invalid_time | ((F.coalesce(F.col(col), F.lit("")) != "") & F.col(f"{col}_utc").isNull())
    error = (F.when(~F.coalesce(F.col("airline").rlike("^[A-Z0-9]{3}$"), F.lit(False)), "invalid_airline")
             .when(~F.coalesce(F.col("flight_number").rlike("^[A-Z0-9]{1,8}$"), F.lit(False)), "invalid_flight_number")
             .when(~F.coalesce(F.col("origin").rlike("^[A-Z0-9]{4}$"), F.lit(False)) | ~F.coalesce(F.col("destination").rlike("^[A-Z0-9]{4}$"), F.lit(False)), "invalid_airport")
             .when(~F.coalesce(F.col("status").isin("REALIZADO", "CANCELADO", "NAO_INFORMADO"), F.lit(False)), "unknown_status")
             .when(invalid_time, "invalid_timestamp")
             .when(F.col("reference_date_parsed").isNull(), "invalid_reference_date")
             .when(F.date_format("reference_date_parsed", "yyyy-MM") != period, "reference_month_mismatch"))
    frame = frame.withColumn("error", error).withColumn("source_month", F.lit(period)).withColumn("source_sha256", F.lit(digest))
    identity = [F.col(c) for c in ["airline", "flight_number", "di", "line_type", "origin", "destination", "reference_date"]]
    # Reference date also identifies unplanned flights without a scheduled departure.
    identity.append(F.coalesce(F.col("scheduled_departure"), F.col("actual_departure"), F.lit("")))
    frame = frame.withColumn("flight_id", F.sha2(F.to_json(F.struct(*identity)), 256))
    frame = frame.withColumn("row_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in selected.columns])), 256))
    return frame


def transform_month(root: Path, source: dict, spark: SparkSession, max_reject_rate: float = .02) -> dict:
    period, digest = source["period"], source["sha256"]
    path = root / source["path"]
    if sha256(path) != digest:
        raise ValueError("Bronze checksum mismatch")
    frame = normalize(spark, path, period, digest).cache()
    try:
        raw_count = frame.count()
        if raw_count == 0:
            raise ValueError("Empty monthly snapshot")
        candidates = frame.filter("error is null")
        conflicts = candidates.groupBy("flight_id").agg(F.countDistinct("row_hash").alias("versions")).filter("versions > 1").select("flight_id")
        marked = frame.join(conflicts.withColumn("conflict", F.lit(True)), "flight_id", "left")
        rejected = marked.withColumn("error", F.when(F.col("conflict"), F.lit("conflicting_business_key")).otherwise(F.col("error"))).filter("error is not null")
        rejected_count = rejected.count()
        quarantine = root / "data" / "quarantine" / period / digest
        rejected.write.mode("overwrite").parquet(str(quarantine))
        reasons = {row.error: row["count"] for row in rejected.groupBy("error").count().collect()}
        if rejected_count / raw_count > max_reject_rate:
            raise ValueError(f"Quarantine rate {rejected_count / raw_count:.2%} exceeds {max_reject_rate:.2%}; source={period}, reasons={reasons}")
        clean = candidates.join(conflicts, "flight_id", "left_anti").dropDuplicates(["flight_id"])
        clean = clean.select("flight_id", "airline", "flight_number", "di", "line_type", "origin", "destination", "status", "justification",
                             "scheduled_departure_utc", "actual_departure_utc", "scheduled_arrival_utc", "actual_arrival_utc",
                             F.col("reference_date_parsed").alias("flight_date"), "source_month", "source_sha256", "row_hash")
        clean = clean.withColumn("departure_delay_min", (F.unix_timestamp("actual_departure_utc") - F.unix_timestamp("scheduled_departure_utc")) / 60.0)
        clean = clean.withColumn("arrival_delay_min", (F.unix_timestamp("actual_arrival_utc") - F.unix_timestamp("scheduled_arrival_utc")) / 60.0)
        delta_path = root / "data" / "silver" / "flights"
        with tempfile.TemporaryDirectory(dir=root / "data") as temp:
            staging = Path(temp) / "rows"
            clean.repartition(2).write.mode("overwrite").parquet(str(staging))
            table = ds.dataset(staging, format="parquet").to_table()
            accepted = table.num_rows
            if accepted == 0:
                raise ValueError("No accepted rows; partition replacement refused")
            if (delta_path / "_delta_log").exists():
                old = DeltaTable(delta_path).to_pyarrow_table(partitions=[("source_month", "=", period)], columns=["flight_id", "row_hash"])
                before = dict(zip(old.column("flight_id").to_pylist(), old.column("row_hash").to_pylist()))
            else:
                before = {}
            after = dict(zip(table.column("flight_id").to_pylist(), table.column("row_hash").to_pylist()))
            changes = {"inserted": len(after.keys() - before.keys()), "removed": len(before.keys() - after.keys()),
                       "updated": sum(before[k] != after[k] for k in before.keys() & after.keys())}
            write_deltalake(delta_path, table, mode="overwrite", partition_by=["source_month"], predicate=f"source_month = '{period}'", target_file_size=32 * 1024 * 1024)
        result = {"period": period, "input_rows": raw_count, "accepted_rows": accepted, "quarantined_rows": rejected_count,
                  "duplicates_removed": raw_count - rejected_count - accepted, "reasons": reasons, **changes,
                  "source_sha256": digest, "delta_version": DeltaTable(delta_path).version()}
        atomic_json(root / "data" / "quality" / f"{period}.json", result)
        return result
    finally:
        frame.unpersist()
