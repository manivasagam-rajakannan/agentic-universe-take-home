# Submission

## Candidate

- **Name** — Manivasagam Rajakannan
- **Assignment** — A, Interaction Ledger and Reporting Pipeline
- **Time spent** — Approximately 45 minutes elapsed for AI-assisted implementation and verification. Candidate review time was not tracked.

## How to run

Use Python 3.11 or newer with SQLite JSON functions. Assignment A uses the standard library.
Run these commands from the repository root in the same shell. Keep the key stable for every
import into the same database.

```bash
export LEDGER_HMAC_KEY="$(python -c 'import secrets; print(secrets.token_hex(32))')"
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
python -m solution ingest data/drop_1
python -m solution ingest data/drop_2
python -m solution report --out reports
python -m unittest discover -v
```

In PowerShell, set the key with
`$env:LEDGER_HMAC_KEY = python -c "import secrets; print(secrets.token_hex(32))"`.
The remaining commands work in either shell. A fresh database can also ingest both drops with
`python -m solution --db combined.sqlite3 ingest data`. Use `--db` before the subcommand.

## Design

- `drops` tracks a digest of the input paths and bytes, plus quality counts for each import.
  A single SQLite transaction commits the drop, its leads, its rejections and its new events.
- `leads` uses workflow and a keyed HMAC of the canonical phone as its primary key. A merge keeps
  the original batch and patches only populated values. `lead_details` applies workflow defaults
  after the merge. The database retains the phone mask, reference and non-PII attributes.
- `events` stores the normalized event facts and is keyed by event ID. The database rejects
  updates and deletes through the application connection. The raw payload and customer name are
  discarded. A report builds a temporary link table against the current leads, including the
  call-to-lead link needed for overrides. Events with no link stay in the ledger and count as
  orphans until enough data arrives.
- A report reads one database snapshot. It groups linked events into calls, selects effective
  dispositions, and aggregates calls by lead, original batch and IST date. The daily success count
  uses a set of lead IDs per workflow and date.

## Decisions and assumptions

| Decision | Rationale |
| --- | --- |
| SQLite with a separate HMAC key | One local database is sufficient for this pipeline. A secret-key digest lets a lead and its events match without retaining a full phone number. |
| Treat each immutable drop as a delivery | The same paths and bytes are a replay even if the drop is moved. A corrected drop is a new delivery. A parent directory is processed in lexical subdirectory order. |
| First nonblank alias from left to right | Gives deterministic behavior if a CSV supplies two columns for one field. Decimal ties round half up. |
| Earliest usable initiation and chronological dispositions | With multiple initiation events for one call, the earliest fixes its date. An event ID breaks disposition timestamp ties. A call ID breaks initiation timestamp ties between calls. |
| Link a phone-less override only to a unique lead within its workflow and call | A conflicting call-to-lead mapping leaves the override quarantined. A linked, unknown disposition contributes a quality count and is excluded from the effective disposition. |
| Export one row per batch that owns an accepted lead | Batch totals come from the lead table. Rejected-only batches are visible in the rejection report. |

## Failure modes considered

- Duplicate deliveries and event IDs leave business metrics unchanged. A malformed JSON line is
  counted and skipped. A structurally broken CSV or a file that changes during ingest rolls back
  the whole drop. SQLite serializes writers, and a key mismatch aborts an import before it changes
  any data.
- A ledger event with an ID and an unusable timestamp can still link. It supplies no initiation
  date or timestamped disposition. Reports use catalogued codes only.
- Outputs are staged before replacing the final CSV and JSON files. Each file replacement is atomic.
  A crash between replacements can leave a mixture of two report runs in the output directory.
- Report generation revisits ledger events to reflect late leads and overrides. It keeps derived
  call histories in memory. Input files remain the caller's responsibility, and rotating the HMAC
  key requires rebuilding from those inputs.

## Testing

- Five tests cover phone, date and amount normalization, IST midnight, alias matching, rejection
  order, duplicate merges and defaults, late-arriving leads, override precedence, all four lead
  buckets, database rollback, key mismatch and the exact five report files on a small fixture.
- Ran the supplied generator with small seeds 42 and 1597. Sequential ingest, re-ingest and ingest
  from a parent directory produced byte-identical final reports for each seed.
- Verification used the contract fixture and generated data. The recruiter's private reference
  outputs and grader were unavailable.

## Results

| Synthetic run | Input lead rows | Final leads | Event lines |
| --- | ---: | ---: | ---: |
| Seed 42, small | 2,057 | 1,892 | 8,745 |
| Seed 731, large | 144,073 | 132,933 | 632,817 |

On the test environment (Python 3.12.14, SQLite 3.53.1), the large run took 43.7 seconds to ingest
and 15.8 seconds to report, excluding data generation. Seed 42 had 1,123 orphans after the first
drop and 462 after both drops, even as more events arrived.

## If I had another day

1. Publish all five outputs together using versioned directories and an atomic manifest switch.
2. Track which leads, calls and dates each new drop affects so larger workloads can update report
   facts without a full rebuild.
3. Add controlled backfills and key rotation backed by a protected input archive.
