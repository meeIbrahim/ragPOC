CREATE TABLE IF NOT EXISTS outbox (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    hash       TEXT NOT NULL REFERENCES document_store(hash) ON DELETE CASCADE,
    status     TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN (
        'PENDING', 'CLAIMED', 'PUBLISHED', 'BLOCKED'
    )),
    claimed_by TEXT,
    claimed_at TIMESTAMP,
    retries    INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- normal sweep: oldest PENDING first
CREATE INDEX IF NOT EXISTS idx_outbox_status_created_at ON outbox(status, created_at);
-- recovery sweep: stale CLAIMED rows
CREATE INDEX IF NOT EXISTS idx_outbox_status_claimed_at ON outbox(status, claimed_at);
