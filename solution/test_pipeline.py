"""Contract tests with small synthetic fixtures and exact expected reports."""

from contextlib import closing
import csv
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest

from solution.ingest import connect, ingest_drop, initialize, read_schemas
from solution.normalization import canonical_phone, ist_timestamp, normalize_date, normalize_decimal
from solution.report import generate_reports


WORKFLOW = "motor_renewal"
KEY = b"fixed-synthetic-fixture-key-used-only-in-tests"
HEADER = ["customer_name", "phone", "policy_number", "policy_expiry", "premium_amount", "language"]
ALIAS_HEADER = ["MOBILE__NO", "Language", "Policy No", "Insured Name", "Expiry", "Premium", "Note"]
LEAD_ONE = "9876543210"
LEAD_TWO = "8765432109"


def event(event_id, kind, call, phone=None, at="2026-10-03T19:15:00Z", code=None):
    result = {
        "event_id": event_id,
        "event_type": kind,
        "workflow_id": WORKFLOW,
        "call_id": call,
        "occurred_at": at,
        "payload": {"disposition_code": code} if code else {},
    }
    if phone is not None:
        result["phone"] = phone
    return result


def write_leads(drop, batch, header, rows, workflow=WORKFLOW):
    path = drop / "leads" / workflow / f"{batch}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as output:
        csv.writer(output).writerows([header, *rows])
    return path


def write_events(drop, events):
    path = drop / "events" / "events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [item if isinstance(item, str) else json.dumps(item) for item in events]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def read_rows(path):
    with path.open(encoding="utf-8", newline="") as source:
        return list(csv.reader(source))


class NormalizationTests(unittest.TestCase):
    def test_phone_formats(self):
        for value in (LEAD_ONE, "+91 98765 43210", "09876543210", "91-98765-43210",
                      "(+91) 9876543210", "+919876543210"):
            with self.subTest(value=value):
                self.assertEqual(canonical_phone(value), "+91" + LEAD_ONE)
        for value in ("123", "5876543210", "+1 415 555 0134", "N/A", None):
            with self.subTest(value=value):
                self.assertIsNone(canonical_phone(value))

    def test_date_decimal_and_ist_rollover(self):
        for value in ("2026-12-01", "01/12/2026", "01-12-2026", "01-dEc-2026"):
            self.assertEqual(normalize_date(value), "2026-12-01")
        self.assertIsNone(normalize_date("31/02/2026"))
        self.assertEqual(normalize_decimal("₹1,234.565"), "1234.57")
        self.assertEqual(normalize_decimal("rS. 1,23,456"), "123456.00")
        self.assertEqual(normalize_decimal("INR 0"), "0.00")
        for value in ("-1", "NaN", "1,2", "1.2.3"):
            self.assertIsNone(normalize_decimal(value))
        expected = "2026-10-04T00:45:00.000000+05:30"
        for value in ("2026-10-03T19:15:00Z", "2026-10-04T00:45:00+05:30", "2026-10-04 00:45:00"):
            self.assertEqual(ist_timestamp(value), expected)


class PipelineTests(unittest.TestCase):
    def setUp(self):
        workspace = self.enterContext(TemporaryDirectory())
        self.root = Path(workspace)
        self.database = self.root / "ledger.sqlite3"
        self.db = self.enterContext(closing(connect(self.database)))
        initialize(self.db)
        self.schemas = read_schemas()

    def test_reports_after_late_lead_and_supervisor_override(self):
        first, second = self.root / "drop_1", self.root / "drop_2"
        write_leads(first, "batch_001", HEADER, [
            ["Example Private Name", LEAD_ONE, "MOT-OLD", "01/12/2026", "INR 14.125", "HI"],
            ["  ", "", "", "  ", "", ""],
            ["", "bad-phone", "MOT-X", "invalid", "invalid", "invalid"],
            ["Test Person", "bad-phone", "", "", "", ""],
            ["No Calls", "7654321098", "MOT-3", "", "", ""],
        ])
        write_leads(second, "batch_003", ALIAS_HEADER, [
            ["(+91) 98765 43210", "invalid", "MOT-1", "New Private Name", "31/02/2026", "", "ignored"],
            ["0" + LEAD_TWO, "", "MOT-2", "Late Lead", "", "", ""],
        ])
        first_initiated = event("e1", "call.initiated", "c1", "+91" + LEAD_ONE)
        write_events(first, [
            event("e8", "disposition.override", "c1", at="2026-10-05T00:10:00Z", code="renewed"),
            first_initiated,
            event("e2", "call.connected", "c1", LEAD_ONE, "2026-10-04 00:45:12"),
            event("e3", "disposition.set", "c1", LEAD_ONE, "2026-10-05T12:00:00Z", "not_interested"),
            event("e9", "disposition.override", "c1", at="2026-10-05T07:00:00+05:30", code="typo"),
            event("e7", "disposition.override", "c1", at="2026-10-04T00:00:00Z", code="dnd"),
            event("e4", "call.initiated", "c2", LEAD_TWO),
            event("e5", "call.telephony_failed", "c2", LEAD_TWO),
            event("e6", "call.connected", "c-missing-initiation", LEAD_TWO),
            event("e10", "disposition.set", "orphan", "9988776655", code="typo"),
            "{broken",
            "{}",
            " ",
        ])
        write_events(second, [
            first_initiated,
            event("e11", "call.initiated", "c3", LEAD_ONE, "2026-10-04T20:00:00+05:30"),
            event("e12", "disposition.set", "c3", LEAD_ONE, "2026-10-04 20:03:00", "renewed"),
            event("e13", "call.initiated", "c4", LEAD_ONE, "2026-10-05 02:01:01"),
        ])

        self.assertTrue(ingest_drop(self.db, KEY, first, self.schemas))
        early_reports = self.root / "early"
        generate_reports(self.db, early_reports)
        self.assertEqual(json.loads((early_reports / "data_quality.json").read_text())["orphan_events"], 4)

        self.assertTrue(ingest_drop(self.db, KEY, second, self.schemas))
        output = self.root / "final"
        generate_reports(self.db, output)
        self.assertEqual(read_rows(output / "lead_report.csv"), [
            ["workflow_id", "external_ref", "batch_id", "phone_masked", "bucket", "total_calls",
             "connected_calls", "last_call_at", "current_disposition", "ever_success"],
            [WORKFLOW, "MOT-1", "batch_001", "+91******3210", "completed", "3", "1",
             "2026-10-05T02:01:01+05:30", "renewed", "true"],
            [WORKFLOW, "MOT-2", "batch_003", "+91******2109", "in_progress_telephony_failed",
             "1", "0", "2026-10-04T00:45:00+05:30", "", "false"],
            [WORKFLOW, "MOT-3", "batch_001", "+91******1098", "yet_to_reach_out", "0", "0", "", "", "false"],
        ])
        self.assertEqual(read_rows(output / "daily_overview.csv"), [
            ["workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate", "success_leads"],
            [WORKFLOW, "2026-10-04", "3", "1", "0.3333", "1"],
            [WORKFLOW, "2026-10-05", "1", "0", "0.0000", "0"],
        ])
        self.assertEqual(read_rows(output / "batch_summary.csv"), [
            ["workflow_id", "batch_id", "total_leads", "yet_to_reach_out",
             "in_progress_telephony_failed", "in_progress_dialed", "completed"],
            [WORKFLOW, "batch_001", "2", "1", "0", "0", "1"],
            [WORKFLOW, "batch_003", "1", "0", "1", "0", "0"],
        ])
        self.assertEqual(read_rows(output / "rejections.csv"), [
            ["workflow_id", "batch_id", "row_number", "reason"],
            [WORKFLOW, "batch_001", "2", "missing_required:customer_name"],
            [WORKFLOW, "batch_001", "3", "invalid_phone"],
        ])
        self.assertEqual(json.loads((output / "data_quality.json").read_text()), {
            "lead_rows_total": 6,
            "lead_rows_rejected": 2,
            "rejections_by_reason": {"missing_required:customer_name": 1, "invalid_phone": 1},
            "duplicate_lead_rows_merged": 1,
            "field_warnings": 2,
            "events_total": 16,
            "malformed_events": 2,
            "duplicate_events_dropped": 1,
            "orphan_events": 1,
            "unknown_disposition_events": 1,
        })
        merged = self.db.execute("SELECT attributes FROM lead_details WHERE external_ref = 'MOT-1'").fetchone()
        self.assertEqual(json.loads(merged[0]), {
            "preferred_language": "hi", "policy_expiry": "2026-12-01", "premium_amount": "14.13",
        })
        defaulted = self.db.execute("SELECT attributes FROM lead_details WHERE external_ref = 'MOT-2'").fetchone()
        self.assertEqual(json.loads(defaulted[0])["preferred_language"], "en")

        before = {p.name: p.read_bytes() for p in output.iterdir()}
        self.assertFalse(ingest_drop(self.db, KEY, first, self.schemas))
        self.assertFalse(ingest_drop(self.db, KEY, second, self.schemas))
        generate_reports(self.db, output)
        self.assertEqual({p.name: p.read_bytes() for p in output.iterdir()}, before)
        for raw in (LEAD_ONE, LEAD_TWO, "Example Private Name", "New Private Name", "Late Lead"):
            self.assertNotIn(raw.encode(), self.database.read_bytes())
        for mutation in ("DELETE FROM events", "UPDATE events SET event_type = 'changed'"):
            with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                self.db.execute(mutation)
            self.db.rollback()

    def test_dialed_bucket_and_workflow_scoped_phone(self):
        drop = self.root / "drop"
        write_leads(drop, "batch_001", HEADER, [["A", LEAD_ONE, "MOT-1", "", "", ""]])
        write_leads(drop, "batch_001", ["Name", "Contact", "Order"], [["B", LEAD_ONE, "ORD-1"]],
                    workflow="policy_feedback")
        write_events(drop, [event("e1", "call.initiated", "c1", LEAD_ONE)])
        ingest_drop(self.db, KEY, drop, self.schemas)
        output = self.root / "final"
        generate_reports(self.db, output)
        rows = read_rows(output / "lead_report.csv")
        self.assertEqual([row[4] for row in rows[1:]], ["in_progress_dialed", "yet_to_reach_out"])
        self.assertEqual(self.db.execute("SELECT COUNT(DISTINCT phone_token) FROM leads").fetchone()[0], 2)

    def test_bad_drop_rolls_back_and_different_key_cannot_ingest(self):
        drop = self.root / "drop"
        path = write_leads(drop, "batch_001", HEADER, [["A", LEAD_ONE, "MOT-1", "", "", ""]])
        with path.open("a", encoding="utf-8") as output:
            output.write('"unclosed')
        with self.assertRaises(csv.Error):
            ingest_drop(self.db, KEY, drop, self.schemas)
        for table in ("leads", "drops", "ledger_key"):
            self.assertEqual(self.db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)
        write_leads(drop, "batch_001", HEADER, [["A", LEAD_ONE, "MOT-1", "", "", ""]])
        self.assertTrue(ingest_drop(self.db, KEY, drop, self.schemas))
        with self.assertRaisesRegex(ValueError, "differs"):
            ingest_drop(self.db, b"a-different-key", drop, self.schemas)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 1)
