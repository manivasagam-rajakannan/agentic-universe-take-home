"""Command line entry point for Assignment A."""

import argparse
from contextlib import closing
import csv
import os
from pathlib import Path
import sqlite3

from .ingest import connect, ingest_drop, initialize, input_drops, read_schemas
from .report import generate_reports


def parse_args():
    parser = argparse.ArgumentParser(description="Interaction Ledger and Reporting Pipeline")
    parser.add_argument("--db", type=Path, default=Path("ledger.sqlite3"), help="SQLite database path")
    commands = parser.add_subparsers(dest="command", required=True)
    ingest = commands.add_parser("ingest", help="Ingest a drop or a parent directory of drops")
    ingest.add_argument("input", type=Path)
    report = commands.add_parser("report", help="Write the five reports")
    report.add_argument("--out", type=Path, required=True)
    return parser, parser.parse_args()


def main():
    parser, args = parse_args()
    try:
        if args.command == "ingest":
            key = os.environ.get("LEDGER_HMAC_KEY", "").encode("utf-8")
            if len(key) < 32:
                raise ValueError("Set LEDGER_HMAC_KEY to a stable random value of at least 32 bytes")
            drops, schemas = input_drops(args.input), read_schemas()
            args.db.parent.mkdir(parents=True, exist_ok=True)
            with closing(connect(args.db)) as db:
                initialize(db)
                for drop in drops:
                    status = "Ingested" if ingest_drop(db, key, drop, schemas) else "Already ingested"
                    print(f"{status} {drop}")
        else:
            if not args.db.is_file():
                raise ValueError(f"Database does not exist: {args.db}")
            with closing(connect(args.db)) as db:
                generate_reports(db, args.out)
            print(f"Wrote five reports to {args.out}")
    except (OSError, ValueError, csv.Error, sqlite3.Error) as error:
        parser.exit(1, f"Error: {error}\n")


if __name__ == "__main__":
    main()
