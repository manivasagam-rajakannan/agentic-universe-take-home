"""Link ledger events and export a consistent snapshot of all five reports."""

from collections import Counter, defaultdict
from contextlib import closing
import csv
from dataclasses import dataclass
from datetime import datetime
from itertools import groupby
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from .ingest import DROP_COUNTERS, SCHEMAS


BUCKETS = (
    "yet_to_reach_out",
    "in_progress_telephony_failed",
    "in_progress_dialed",
    "completed",
)
BATCH_FIELDS = ("workflow_id", "batch_id", "total_leads", *BUCKETS)
DAILY_FIELDS = (
    "workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate", "success_leads",
)
LEAD_FIELDS = (
    "workflow_id", "external_ref", "batch_id", "phone_masked", "bucket", "total_calls",
    "connected_calls", "last_call_at", "current_disposition", "ever_success",
)
REJECTION_FIELDS = ("workflow_id", "batch_id", "row_number", "reason")


@dataclass(frozen=True, slots=True)
class Call:
    call_id: str
    initiated_at: str
    connected: bool
    telephony_failed: bool
    disposition: str | None
    terminal: bool
    success: bool


@dataclass(slots=True)
class Day:
    total: int
    connected: int
    successful_leads: set


def load_dispositions(db):
    db.execute(
        """CREATE TEMP TABLE disposition_catalogue (
           workflow_id TEXT, code TEXT, terminal INTEGER, success INTEGER,
           PRIMARY KEY (workflow_id, code))"""
    )
    with (SCHEMAS / "dispositions.csv").open(encoding="utf-8", newline="") as source:
        db.executemany(
            "INSERT INTO disposition_catalogue VALUES (?, ?, ?, ?)",
            ((row["workflow_id"], row["code"], row["is_terminal"].lower() == "true",
              row["is_success"].lower() == "true") for row in csv.DictReader(source)),
        )


def link_events(db):
    db.execute("CREATE TEMP TABLE event_links (event_id TEXT PRIMARY KEY, phone_token TEXT NOT NULL)")
    db.execute(
        """INSERT INTO event_links
           SELECT e.event_id, e.phone_token FROM events e
           JOIN leads l USING (workflow_id, phone_token)
           WHERE e.event_type <> 'disposition.override'"""
    )
    db.execute(
        """CREATE TEMP TABLE call_leads AS
           SELECT e.workflow_id, e.call_id, MIN(link.phone_token) AS phone_token
           FROM events e JOIN event_links link USING (event_id)
           WHERE e.call_id IS NOT NULL
           GROUP BY e.workflow_id, e.call_id
           HAVING COUNT(DISTINCT link.phone_token) = 1"""
    )
    db.execute("CREATE UNIQUE INDEX temp.call_leads_by_call ON call_leads (workflow_id, call_id)")
    db.execute(
        """INSERT INTO event_links
           SELECT e.event_id, cl.phone_token FROM events e
           JOIN call_leads cl USING (workflow_id, call_id)
           WHERE e.event_type = 'disposition.override'"""
    )


def call_identity(row):
    return row["workflow_id"], row["call_id"], row["phone_token"]


def summarize_call(call_id, events):
    initiated = latest_set = latest_override = None
    connected = telephony_failed = False
    for event in events:
        kind = event["event_type"]
        stamp = event["occurred_at"]
        if kind == "call.initiated" and stamp is not None and initiated is None:
            initiated = stamp
        elif kind == "call.connected":
            connected = True
        elif kind == "call.telephony_failed":
            telephony_failed = True
        elif stamp is not None and event["terminal"] is not None:
            disposition = (event["disposition_code"], bool(event["terminal"]), bool(event["success"]))
            if kind == "disposition.override":
                latest_override = disposition
            elif kind == "disposition.set":
                latest_set = disposition
    if initiated is None:
        return None
    code, terminal, success = latest_override or latest_set or (None, False, False)
    return Call(call_id, initiated, connected, telephony_failed, code, terminal, success)


def calls_by_lead(db):
    rows = db.execute(
        """SELECT e.workflow_id, e.call_id, l.phone_token, e.event_type, e.occurred_at,
                  e.disposition_code, d.terminal, d.success
           FROM events e JOIN event_links l USING (event_id)
           LEFT JOIN disposition_catalogue d
             ON d.workflow_id = e.workflow_id AND d.code = e.disposition_code
           WHERE e.call_id IS NOT NULL
           ORDER BY e.workflow_id, e.call_id, l.phone_token, e.occurred_at, e.event_id"""
    )
    result = defaultdict(list)
    for (workflow, call_id, token), events in groupby(rows, key=call_identity):
        call = summarize_call(call_id, events)
        if call is not None:
            result[workflow, token].append(call)
    return result


def lead_bucket(calls):
    if not calls:
        return "yet_to_reach_out"
    if any(call.terminal for call in calls):
        return "completed"
    if all(call.telephony_failed for call in calls):
        return "in_progress_telephony_failed"
    return "in_progress_dialed"


def lead_rows(db, calls, batches, days):
    leads = db.execute(
        """SELECT workflow_id, phone_token, external_ref, batch_id, phone_masked FROM leads
           ORDER BY workflow_id, external_ref, phone_masked, phone_token"""
    )
    for workflow, token, reference, batch, masked in leads:
        history = sorted(calls.get((workflow, token), ()), key=lambda c: (c.initiated_at, c.call_id))
        bucket = lead_bucket(history)
        batches[workflow, batch][bucket] += 1
        for call in history:
            day = days[workflow, call.initiated_at[:10]]
            day.total += 1
            day.connected += call.connected
            if call.success:
                day.successful_leads.add(token)
        last_call = datetime.fromisoformat(history[-1].initiated_at).isoformat() if history else ""
        current = next((call.disposition for call in reversed(history) if call.disposition), "")
        yield (workflow, reference, batch, masked, bucket, len(history),
               sum(call.connected for call in history), last_call, current,
               "true" if any(call.success for call in history) else "false")


def batch_rows(batches):
    for (workflow, batch), counts in sorted(batches.items()):
        yield workflow, batch, counts.total(), *(counts[bucket] for bucket in BUCKETS)


def daily_rows(days):
    for (workflow, day), totals in sorted(days.items()):
        yield (workflow, day, totals.total, totals.connected,
               f"{totals.connected / totals.total:.4f}", len(totals.successful_leads))


def write_csv(path, fields, rows):
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(fields)
        writer.writerows(rows)


def quality_counts(db):
    totals = dict(db.execute(
        "SELECT " + ", ".join(f"COALESCE(SUM({key}), 0) AS {key}" for key in DROP_COUNTERS)
        + " FROM drops"
    ).fetchone())
    reasons = dict(db.execute(
        "SELECT reason, COUNT(*) FROM rejections GROUP BY reason ORDER BY reason"
    ).fetchall())
    orphan_count = db.execute(
        """SELECT (SELECT COUNT(*) FROM events) - (SELECT COUNT(*) FROM event_links)"""
    ).fetchone()[0]
    unknown_count = db.execute(
        """SELECT COUNT(*) FROM events e JOIN event_links link USING (event_id)
           LEFT JOIN disposition_catalogue d
             ON d.workflow_id = e.workflow_id AND d.code = e.disposition_code
           WHERE e.event_type GLOB 'disposition.*' AND d.code IS NULL"""
    ).fetchone()[0]
    return {
        **totals,
        "lead_rows_rejected": sum(reasons.values()),
        "rejections_by_reason": reasons,
        "orphan_events": orphan_count,
        "unknown_disposition_events": unknown_count,
    }


def generate_reports(db, destination):
    destination.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".report-", dir=destination) as staging:
        temporary = Path(staging)
        with db:
            db.execute("BEGIN")
            load_dispositions(db)
            link_events(db)
            batches = defaultdict(Counter)
            days = defaultdict(lambda: Day(0, 0, set()))
            write_csv(temporary / "lead_report.csv", LEAD_FIELDS,
                      lead_rows(db, calls_by_lead(db), batches, days))
            write_csv(temporary / "batch_summary.csv", BATCH_FIELDS, batch_rows(batches))
            write_csv(temporary / "daily_overview.csv", DAILY_FIELDS, daily_rows(days))
            with closing(db.execute(
                """SELECT workflow_id, batch_id, row_number, reason FROM rejections
                   ORDER BY workflow_id, batch_id, row_number, drop_id"""
            )) as rows:
                write_csv(temporary / "rejections.csv", REJECTION_FIELDS, rows)
            (temporary / "data_quality.json").write_text(
                json.dumps(quality_counts(db), indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            for table in ("event_links", "call_leads", "disposition_catalogue"):
                db.execute(f"DROP TABLE temp.{table}")
        for path in sorted(temporary.iterdir()):
            path.replace(destination / path.name)
