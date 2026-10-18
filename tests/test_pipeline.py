import json
import tempfile
import unittest
from datetime import timezone
from pathlib import Path

import duckdb
from filelock import FileLock, Timeout

from flightledger.pipeline import sync
from flightledger.gold import export_s3
from flightledger.storage import read_json, sha256
from helpers import fixtures, read_month, write_month


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.tmp.name)
        cls.inputs = fixtures(cls.root)
        cls.first = sync(cls.root, ["2025-01", "2025-02"], cls.inputs)

    @classmethod
    def tearDownClass(cls): cls.tmp.cleanup()

    def query(self, sql):
        published = read_json(self.root / "data/published.json")
        with duckdb.connect(str(self.root / published["warehouse"]), read_only=True) as conn:
            return conn.execute(sql).fetchall()

    def test_01_noop_preserves_published_pointer(self):
        result = sync(self.root, ["2025-01", "2025-02"], self.inputs)
        self.assertEqual(result["status"], "NOOP")
        self.assertEqual(result["snapshot_id"], self.first["snapshot_id"])
        self.assertEqual(self.query("select count(*) from analytics.fct_flight")[0][0], 24)

    def test_02_timezone_and_metric_denominators(self):
        row = self.query("select scheduled_departure_utc, departure_delay_min, arrival_delay_min from analytics.fct_flight limit 1")[0]
        self.assertEqual(row[0].astimezone(timezone.utc).hour, 13)
        self.assertEqual(row[1:], (20.0, 10.0))
        self.assertEqual(self.query("select departure_otp15, arrival_otp15 from analytics.mart_route_reliability limit 1")[0], (0.0, 1.0))

    def test_03_revised_partition_replaces_removed_group(self):
        path = self.inputs / "VRA_2025_01.csv"
        rows = read_month(path)
        # Remove a route entirely; an incremental model must not keep its old aggregate.
        for row in rows: row["Sigla ICAO Aeroporto Destino"] = "SBTE"
        rows = rows[:-1]
        rows[0]["Situação Voo"] = "CANCELADO"
        rows[0]["Partida Real"] = rows[0]["Chegada Real"] = ""
        rows[1]["Número Voo"] = "Z0406"
        rows[1]["Partida Prevista"] = ""
        write_month(path, rows)
        result = sync(self.root, ["2025-01"], self.inputs)
        self.assertEqual(result["changed_months"], ["2025-01"])
        self.assertEqual(self.query("select count(*) from analytics.fct_flight")[0][0], 23)
        self.assertEqual(self.query("select count(*) from analytics.fct_daily_operations where source_month='2025-01' and destination='SBGR'")[0][0], 0)
        self.assertEqual(self.query("select sum(flights),sum(cancelled),sum(departure_observed) from analytics.fct_daily_operations where source_month='2025-01'")[0], (11,1,9))
        self.assertEqual(self.query("select count(*) from analytics.fct_flight where source_month='2025-02'")[0][0], 12)

    def test_04_quality_failure_preserves_published_warehouse(self):
        before = read_json(self.root / "data/published.json")
        path = self.inputs / "VRA_2025_01.csv"; rows = read_month(path)
        original = path.read_bytes()
        try:
            rows[0]["Sigla ICAO Aeroporto Origem"] = "INVALID"
            write_month(path, rows)
            with self.assertRaisesRegex(ValueError, "Quarantine rate"):
                sync(self.root, ["2025-01"], self.inputs)
            self.assertEqual(before, read_json(self.root / "data/published.json"))
            self.assertEqual(sha256(self.root / before["warehouse"]), before["warehouse_sha256"])
        finally:
            path.write_bytes(original)

    def test_05_failed_gold_recovers_without_redownloading(self):
        before = read_json(self.root / "data/published.json")
        path = self.inputs / "VRA_2025_01.csv"; rows = read_month(path)
        rows[2]["Partida Real"] = "01/01/2025 11:00"
        write_month(path, rows)
        with self.assertRaisesRegex(RuntimeError, "Injected failure"):
            sync(self.root, ["2025-01"], self.inputs, fail_before_publish=True)
        self.assertEqual(before, read_json(self.root / "data/published.json"))
        result = sync(self.root, ["2025-01"], self.inputs)
        self.assertEqual(result["status"], "PUBLISHED")
        self.assertEqual(result["changed_months"], ["2025-01"])

    def test_06_replay_matches_incremental_result(self):
        before = self.query("select * from analytics.fct_daily_operations order by all")
        sync(self.root, [], replay=True)
        self.assertEqual(before, self.query("select * from analytics.fct_daily_operations order by all"))

    def test_07_concurrent_pipeline_is_rejected(self):
        with FileLock(str(self.root / "data/pipeline.lock")):
            with self.assertRaises(Timeout): sync(self.root, ["2025-01"], self.inputs)

    def test_08_s3_manifest_only_after_all_uploads(self):
        class Client:
            def __init__(self, fail=False): self.uploads=[]; self.manifests=[]; self.fail=fail
            def upload_file(self, path, bucket, key):
                self.uploads.append(key)
                if self.fail: raise OSError("upload interrupted")
            def put_object(self, **kw): self.manifests.append(kw)
        good = Client(); result = export_s3(self.root, "test-bucket", client=good)
        self.assertEqual(len(result["objects"]), 4); self.assertEqual(len(good.manifests), 1)
        bad = Client(fail=True)
        with self.assertRaises(OSError): export_s3(self.root, "test-bucket", client=bad)
        self.assertEqual(bad.manifests, [])
