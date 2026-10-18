import csv
import shutil
from pathlib import Path

from flightledger.transform import FIELDS


def fixtures(root: Path, count: int = 12) -> Path:
    shutil.copytree(Path(__file__).resolve().parents[1] / "dbt", root / "dbt")
    directory = root / "input"
    directory.mkdir()
    fields = list(FIELDS)
    for month in ["01", "02"]:
        rows = []
        for i in range(count):
            rows.append(dict(zip(fields, ["TAM", f"{i:04d}", "0", "N", "SBTE", "SBGR",
                f"01/{month}/2025 10:00", f"01/{month}/2025 10:20",
                f"01/{month}/2025 12:00", f"01/{month}/2025 12:10", "REALIZADO",
                f"01/{month}/2025 00:00:00", ""])))
        write_month(directory / f"VRA_2025_{month}.csv", rows)
    airports = ["id", "ident", "type", "name", "latitude_deg", "longitude_deg", "iso_country", "iso_region", "municipality", "icao_code", "iata_code"]
    with (directory / "airports.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f); writer.writerow(airports)
        writer.writerow([1,"SBTE","medium_airport","Teresina",-5.06,-42.82,"BR","BR-PI","Teresina","SBTE","THE"])
        writer.writerow([2,"SBGR","large_airport","Guarulhos",-23.43,-46.47,"BR","BR-SP","Guarulhos","SBGR","GRU"])
    return directory


def read_month(path):
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter=";"))


def write_month(path, rows):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(FIELDS), delimiter=";")
        writer.writeheader(); writer.writerows(rows)
