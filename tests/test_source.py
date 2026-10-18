import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from flightledger.cli import month
from flightledger.source import ingest, download
from flightledger.storage import atomic_json, read_json, sha256
from helpers import fixtures


class SourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name); self.inputs = fixtures(self.root)

    def test_raw_checksum_and_snapshot_reuse(self):
        first = ingest(self.root, ["2025-01"], self.inputs)
        second = ingest(self.root, ["2025-01"], self.inputs)
        self.assertEqual(first["months"]["2025-01"]["sha256"], second["months"]["2025-01"]["sha256"])
        record = first["months"]["2025-01"]
        self.assertEqual(sha256(self.root / record["path"]), record["sha256"])

    def test_incomplete_download_does_not_replace_cache(self):
        target = self.root / "cache.csv"; target.write_bytes(b"previous")
        class Response:
            headers = {"Content-Type": "text/csv", "Content-Length": "1000"}
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self, size):
                if getattr(self, "done", False): return b""
                self.done = True; return b"partial"
        with patch("urllib.request.urlopen", side_effect=lambda *a, **kw: Response()):
            with self.assertRaises(OSError): download("https://example.org/file.csv", target, retries=1)
        self.assertEqual(target.read_bytes(), b"previous")
        self.assertFalse(target.with_suffix(".csv.part").exists())

    def test_html_is_not_accepted_as_csv(self):
        class Response:
            headers = {"Content-Type": "text/html"}
            def __enter__(self): return self
            def __exit__(self, *args): pass
        with patch("urllib.request.urlopen", return_value=Response()):
            with self.assertRaises(ValueError): download("https://example.org/file.csv", self.root / "bad.csv")

    def test_atomic_metadata_and_month_validation(self):
        p = self.root / "meta.json"; atomic_json(p, {"version": 1})
        self.assertEqual(read_json(p)["version"], 1)
        self.assertEqual(month("2025-01"), "2025-01")
        for value in ["2025-13", "2025-1", "2025-01';drop table x"]:
            with self.assertRaises(Exception): month(value)
