import logging
import time
from pathlib import Path

from filelock import FileLock

from .gold import publish
from .source import ingest
from .storage import atomic_json, read_json, sha256
from .transform import session, transform_month

LOG = logging.getLogger(__name__)


def model_hash(root: Path) -> str:
    import hashlib
    content = b"".join(p.relative_to(root).as_posix().encode() + p.read_bytes() for p in sorted((root / "dbt").rglob("*")) if p.is_file() and "target" not in p.parts and "logs" not in p.parts)
    return hashlib.sha256(content).hexdigest()


def sync(root: Path, months: list[str], input_dir: Path | None = None, refresh: bool = False,
         replay: bool = False, max_reject_rate: float = .02, fail_before_publish: bool = False) -> dict:
    root = Path(root).resolve()
    (root / "data").mkdir(exist_ok=True)
    started = time.monotonic()
    with FileLock(str(root / "data" / "pipeline.lock"), timeout=0):
        try:
            index = read_json(root / "data" / "bronze" / "index.json") if replay else ingest(root, months, input_dir, refresh)
            if not index:
                raise ValueError("No Bronze snapshots available for replay")
            for record in [*index["months"].values(), index["airports"]]:
                if sha256(root / record["path"]) != record["sha256"]:
                    raise ValueError("Bronze checksum mismatch")
            silver_path = root / "data" / "silver_state.json"
            silver = read_json(silver_path, {})
            dirty = [m for m, record in index["months"].items() if replay or silver.get(m) != record["sha256"]]
            spark = session() if dirty else None
            for month in sorted(dirty):
                result = transform_month(root, index["months"][month], spark, max_reject_rate)
                LOG.info("month=%s accepted=%s quarantined=%s", month, result["accepted_rows"], result["quarantined_rows"])
                silver[month] = index["months"][month]["sha256"]
                atomic_json(silver_path, silver)
            previous = read_json(root / "data" / "published.json", {})
            changed = [m for m, digest in silver.items() if previous.get("source_hashes", {}).get(m) != digest]
            code_hash = model_hash(root)
            needs_publish = replay or changed or previous.get("airports_hash") != index["airports"]["sha256"] or previous.get("model_hash") != code_hash
            if needs_publish:
                result = publish(root, index, changed, full_refresh=replay or not previous or previous.get("model_hash") != code_hash,
                                 fail_before_publish=fail_before_publish, model_hash=code_hash)
                status = "PUBLISHED"
            else:
                result, status = previous, "NOOP"
            audit = {"status": status, "seconds": round(time.monotonic() - started, 3), "processed_months": dirty, "snapshot_id": result["snapshot_id"]}
            atomic_json(root / "data" / "last_run.json", audit)
            LOG.info("status=%s snapshot=%s elapsed=%.2fs", status, result["snapshot_id"], audit["seconds"])
            return {**result, "status": status}
        except Exception as exc:
            atomic_json(root / "data" / "last_run.json", {"status": "FAILED", "error": str(exc), "seconds": round(time.monotonic() - started, 3)})
            raise
