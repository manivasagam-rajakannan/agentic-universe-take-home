CREATE TABLE IF NOT EXISTS ledger_key (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    fingerprint TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS drops (
    id INTEGER PRIMARY KEY,
    digest TEXT NOT NULL UNIQUE,
    lead_rows_total INTEGER NOT NULL DEFAULT 0,
    duplicate_lead_rows_merged INTEGER NOT NULL DEFAULT 0,
    field_warnings INTEGER NOT NULL DEFAULT 0,
    events_total INTEGER NOT NULL DEFAULT 0,
    malformed_events INTEGER NOT NULL DEFAULT 0,
    duplicate_events_dropped INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS workflows (
    workflow_id TEXT PRIMARY KEY,
    defaults TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS leads (
    workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
    phone_token TEXT NOT NULL,
    phone_masked TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    external_ref TEXT NOT NULL,
    attributes TEXT NOT NULL,
    PRIMARY KEY (workflow_id, phone_token)
);

CREATE VIEW IF NOT EXISTS lead_details AS
SELECT workflow_id, phone_token, phone_masked, batch_id, external_ref,
       json_patch(defaults, attributes) AS attributes
FROM leads JOIN workflows USING (workflow_id);

CREATE TABLE IF NOT EXISTS rejections (
    drop_id INTEGER NOT NULL REFERENCES drops(id),
    workflow_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    row_number INTEGER NOT NULL CHECK (row_number > 0),
    reason TEXT NOT NULL,
    PRIMARY KEY (drop_id, workflow_id, batch_id, row_number)
);

-- No lead foreign key here because events can arrive before the lead.
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT NOT NULL PRIMARY KEY,
    event_type TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    call_id TEXT,
    occurred_at TEXT,
    phone_token TEXT,
    disposition_code TEXT
);

CREATE INDEX IF NOT EXISTS events_by_call
ON events (workflow_id, call_id, occurred_at, event_id);

CREATE TRIGGER IF NOT EXISTS events_no_update
BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(ABORT, 'Interaction events are append-only');
END;

CREATE TRIGGER IF NOT EXISTS events_no_delete
BEFORE DELETE ON events
BEGIN
    SELECT RAISE(ABORT, 'Interaction events are append-only');
END;
