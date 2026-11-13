"""Persist leads, rejections and an append-only event ledger."""

import csv
import hashlib
import json
from pathlib import Path
import sqlite3

from .normalization import (
    canonical_phone,
    header_key,
    ist_timestamp,
    key_fingerprint,
    normalize_field,
    phone_token,
)


SCHEMAS = Path(__file__).resolve().parents[1] / "assignment-a-data" / "schemas"
DROP_COUNTERS = (
    "lead_rows_total",
    "duplicate_lead_rows_merged",
    "field_warnings",
    "events_total",
    "malformed_events",
    "duplicate_events_dropped",
)


def connect(path):
    db = sqlite3.connect(path, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    db.execute("PRAGMA recursive_triggers = ON")
    return db


def initialize(db):
    schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
    db.executescript(schema)


def read_schemas():
    schemas = {}
    for path in sorted(SCHEMAS.glob("*.json")):
        schema = json.loads(path.read_text(encoding="utf-8"))
        schemas[schema["workflow_id"]] = schema["fields"]
    return schemas


def drop_files(root):
    return (
        sorted((root / "leads").glob("*/*.csv"), key=lambda p: (p.name, p.parent.name)),
        sorted((root / "events").glob("*.jsonl")),
    )


def drop_digest(root, files):
    manifest = hashlib.sha256()
    for path in files:
        with path.open("rb") as source:
            content = hashlib.file_digest(source, "sha256").hexdigest()
        relative_path = path.relative_to(root).as_posix()
        manifest.update(json.dumps([relative_path, content]).encode("ascii") + b"\n")
    return manifest.hexdigest()


def input_drops(path):
    if not path.is_dir():
        raise ValueError(f"Input directory does not exist: {path}")
    if (path / "leads").is_dir() or (path / "events").is_dir():
        return [path]
    children = [
        child for child in sorted(path.iterdir())
        if child.is_dir() and ((child / "leads").is_dir() or (child / "events").is_dir())
    ]
    if not children:
        raise ValueError("Expected a drop with leads/ or events/, or a parent of drops")
    return children


def column_indices(header, fields):
    columns = [header_key(column) for column in header]
    for field in fields:
        names = {header_key(name) for name in (field["name"], *field.get("aliases", []))}
        yield [index for index, column in enumerate(columns) if column in names]


def normalize_lead(cells, fields, indices):
    values = {}
    warnings = 0
    for field, matching_columns in zip(fields, indices):
        name = field["name"]
        value = next(
            (cells[index] for index in matching_columns if index < len(cells) and cells[index]),
            "",
        )
        if not value:
            if field.get("required"):
                return None, 0, f"missing_required:{name}"
            continue
        normalized = normalize_field(field, value)
        if normalized is None:
            if field["type"] == "phone":
                return None, 0, "invalid_phone"
            if field.get("required"):
                return None, 0, f"invalid_required:{name}"
            warnings += 1
        else:
            values[name] = normalized
    return values, warnings, None


def merge_lead(db, key, workflow, batch, phone_field, reference_field, public_fields, values):
    phone = values[phone_field]
    token = phone_token(key, workflow, phone)
    attributes = json.dumps(
        {name: values[name] for name in public_fields if name in values}, ensure_ascii=False
    )
    inserted = db.execute(
        """INSERT INTO leads VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT (workflow_id, phone_token) DO NOTHING""",
        (workflow, token, "+91******" + phone[-4:], batch, values[reference_field], attributes),
    )
    if inserted.rowcount:
        return 0
    db.execute(
        """UPDATE leads SET external_ref = ?, attributes = json_patch(attributes, ?)
           WHERE workflow_id = ? AND phone_token = ?""",
        (values[reference_field], attributes, workflow, token),
    )
    return 1


def ingest_leads(db, key, drop_id, paths, schemas, counters):
    for path in paths:
        workflow, batch = path.parent.name, path.stem
        if workflow not in schemas:
            raise ValueError(f"Missing lead schema for workflow: {workflow}")
        fields = schemas[workflow]
        phone_field = next(field["name"] for field in fields if field["type"] == "phone")
        reference_field = next(field["name"] for field in fields if field.get("reference"))
        public_fields = [
            field["name"] for field in fields
            if not field.get("pii") and field["name"] not in (phone_field, reference_field)
        ]
        with path.open(encoding="utf-8-sig", newline="") as source:
            rows = csv.reader(source, strict=True)
            indices = tuple(column_indices(next(rows, []), fields))
            row_number = 0
            for row in rows:
                cells = [cell.strip() for cell in row]
                if not any(cells):
                    continue
                row_number += 1
                counters["lead_rows_total"] += 1
                values, warnings, reason = normalize_lead(cells, fields, indices)
                if reason:
                    db.execute(
                        "INSERT INTO rejections VALUES (?, ?, ?, ?, ?)",
                        (drop_id, workflow, batch, row_number, reason),
                    )
                    continue
                counters["field_warnings"] += warnings
                counters["duplicate_lead_rows_merged"] += merge_lead(
                    db, key, workflow, batch, phone_field, reference_field, public_fields, values
                )


def string_or_none(value):
    return value if isinstance(value, str) and value.strip() else None


def invalid_json_constant(value):
    raise ValueError(f"Invalid JSON constant: {value}")


def ingest_events(db, key, paths, counters):
    for path in paths:
        with path.open("rb") as source:
            for line in source:
                if not line.strip():
                    continue
                counters["events_total"] += 1
                try:
                    event = json.loads(line.decode("utf-8"), parse_constant=invalid_json_constant)
                except (ValueError, RecursionError):
                    event = None
                event_id = string_or_none(event.get("event_id")) if isinstance(event, dict) else None
                if event_id is None:
                    counters["malformed_events"] += 1
                    continue
                workflow = string_or_none(event.get("workflow_id")) or ""
                event_type = string_or_none(event.get("event_type")) or ""
                phone = canonical_phone(event.get("phone"))
                payload = event.get("payload")
                code = string_or_none(payload.get("disposition_code")) if isinstance(payload, dict) else None
                token = phone_token(key, workflow, phone) if phone else None
                inserted = db.execute(
                    """INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT (event_id) DO NOTHING""",
                    (event_id, event_type, workflow, string_or_none(event.get("call_id")),
                     ist_timestamp(event.get("occurred_at")), token, code),
                )
                if not inserted.rowcount:
                    counters["duplicate_events_dropped"] += 1


def ingest_drop(db, key, root, schemas):
    leads, events = drop_files(root)
    if not leads and not events:
        raise ValueError(f"No lead CSVs or event JSONL files in {root}")
    digest = drop_digest(root, leads + events)
    fingerprint = key_fingerprint(key)
    with db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO ledger_key VALUES (1, ?) ON CONFLICT DO NOTHING", (fingerprint,))
        recorded_key = db.execute("SELECT fingerprint FROM ledger_key").fetchone()[0]
        if recorded_key != fingerprint:
            raise ValueError("LEDGER_HMAC_KEY differs from the key used for this database")
        inserted = db.execute("INSERT INTO drops (digest) VALUES (?) ON CONFLICT DO NOTHING", (digest,))
        if not inserted.rowcount:
            return False
        db.executemany(
            """INSERT INTO workflows VALUES (?, ?)
               ON CONFLICT (workflow_id) DO UPDATE SET defaults = excluded.defaults""",
            ((workflow, json.dumps({
                field["name"]: field["default"]
                for field in fields if "default" in field and not field.get("pii")
            })) for workflow, fields in schemas.items()),
        )
        counters = dict.fromkeys(DROP_COUNTERS, 0)
        ingest_leads(db, key, inserted.lastrowid, leads, schemas, counters)
        ingest_events(db, key, events, counters)
        if drop_files(root) != (leads, events) or drop_digest(root, leads + events) != digest:
            raise ValueError(f"Drop files changed during ingestion: {root}")
        db.execute(
            """UPDATE drops SET lead_rows_total = ?, duplicate_lead_rows_merged = ?,
               field_warnings = ?, events_total = ?, malformed_events = ?,
               duplicate_events_dropped = ? WHERE id = ?""",
            (*counters.values(), inserted.lastrowid),
        )
    return True
