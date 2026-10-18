import csv
import hashlib
import shutil
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from .storage import atomic_json, read_json, sha256

ANAC = "https://siros.anac.gov.br/siros/registros/diversos/vra"
AIRPORTS = "https://davidmegginson.github.io/ourairports-data/airports.csv"


def download(url: str, destination: Path, retries: int = 3) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".part")
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "FlightLedger/0.1"})
            with urllib.request.urlopen(req, timeout=90) as response, temporary.open("wb") as out:
                if "html" in response.headers.get("Content-Type", "").lower():
                    raise ValueError("Expected a CSV, received HTML")
                expected = response.headers.get("Content-Length")
                size = 0
                while chunk := response.read(1024 * 1024):
                    size += len(chunk)
                    if size > 160 * 1024 * 1024:
                        raise ValueError("Source exceeds the per-file limit")
                    out.write(chunk)
                if size == 0 or (expected and size != int(expected)):
                    raise OSError("Incomplete source download")
            temporary.replace(destination)
            return
        except urllib.error.HTTPError as error:
            if error.code not in [429, 500, 502, 503, 504] or attempt == retries - 1:
                raise
            time.sleep(min(2 ** attempt, 4))
        except (OSError, TimeoutError):
            if attempt == retries - 1:
                raise
            time.sleep(min(2 ** attempt, 4))
        finally:
            temporary.unlink(missing_ok=True)


def ingest(root: Path, months: list[str], input_dir: Path | None = None, refresh: bool = False) -> dict:
    index_path = root / "data" / "bronze" / "index.json"
    index = read_json(index_path, {"months": {}, "airports": None})
    for period in [*months, "airports"]:
        name = "airports.csv" if period == "airports" else f"VRA_{period.replace('-', '_')}.csv"
        url = AIRPORTS if period == "airports" else f"{ANAC}/{period[:4]}/{name}"
        cached = root / "data" / "downloads" / name
        source = input_dir / name if input_dir else cached
        if input_dir:
            if not source.is_file():
                raise FileNotFoundError(source)
        elif refresh or not cached.exists():
            download(url, cached)
        digest = sha256(source)
        target = root / "data" / "bronze" / period / f"{digest}.csv"
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            shutil.copyfile(source, target)
        if sha256(target) != digest:
            raise ValueError(f"Bronze checksum mismatch: {period}")
        with target.open(encoding="utf-8-sig", newline="") as stream:
            header = next(csv.reader(stream, delimiter="," if period == "airports" else ";"))
        if len(header) < 10:
            raise ValueError(f"Unexpected source header: {period}")
        record = {"period": period, "sha256": digest, "bytes": target.stat().st_size,
                  "url": url, "path": target.relative_to(root).as_posix(),
                  "retrieved_at": datetime.now(timezone.utc).isoformat(), "header": header}
        atomic_json(target.with_suffix(".json"), record)
        if period == "airports":
            index["airports"] = record
        else:
            index["months"][period] = record
    atomic_json(index_path, index)
    return index
