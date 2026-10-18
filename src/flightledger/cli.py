import argparse
import json
import logging
import re
from pathlib import Path

from .gold import export_s3
from .pipeline import sync
from .report import write_reports
from .storage import read_json, sha256


def month(value: str) -> str:
    if not re.fullmatch(r"20[0-9]{2}-(0[1-9]|1[0-2])", value):
        raise argparse.ArgumentTypeError("Use YYYY-MM")
    return value


def main():
    parser = argparse.ArgumentParser(prog="flightledger")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="command", required=True)
    load = commands.add_parser("sync")
    load.add_argument("--months", nargs="+", type=month, required=True)
    load.add_argument("--input-dir", type=Path)
    load.add_argument("--refresh", action="store_true")
    load.add_argument("--max-reject-rate", type=float, default=.02)
    commands.add_parser("replay")
    commands.add_parser("audit")
    commands.add_parser("report")
    export = commands.add_parser("export-s3")
    export.add_argument("--bucket", required=True)
    export.add_argument("--prefix", default="flightledger")
    export.add_argument("--endpoint")
    args = parser.parse_args()
    root = args.root.resolve()
    if not (root / "dbt" / "dbt_project.yml").exists():
        parser.error("Execute na raiz do projeto, ou informe --root")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if args.command == "sync":
        if not 0 <= args.max_reject_rate < 1:
            parser.error("max-reject-rate must be in [0, 1)")
        result = sync(root, args.months, args.input_dir, args.refresh, max_reject_rate=args.max_reject_rate)
    elif args.command == "replay":
        result = sync(root, [], replay=True)
    elif args.command == "export-s3":
        result = export_s3(root, args.bucket, args.prefix, args.endpoint)
    elif args.command == "report":
        result = write_reports(root)
    else:
        result = read_json(root / "data" / "published.json")
        if not result or sha256(root / result["warehouse"]) != result["warehouse_sha256"]:
            raise ValueError("Published warehouse is missing or corrupted")
        print("Published warehouse checksum verified")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
