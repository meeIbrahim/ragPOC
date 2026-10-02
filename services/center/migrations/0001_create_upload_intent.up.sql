CREATE TABLE IF NOT EXISTS upload_intent (
    uuid             TEXT PRIMARY KEY NOT NULL,
    file_name        TEXT NOT NULL,
    temp_bucket_path TEXT,
    -- set by the hash worker; links this intent (job) to its document
    hash             TEXT REFERENCES document_store(hash),
    status           TEXT NOT NULL DEFAULT 'WAITING' CHECK (status IN (
        'WAITING', 'UPLOADED', 'HASHING', 'HASHED', 'EXPIRED', 'FAILED'
    )),
    claimed_by       TEXT,
    claimed_at       TIMESTAMP,
    retries          INTEGER NOT NULL DEFAULT 0,
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at       TIMESTAMP NOT NULL,
    uploaded_at      TIMESTAMP
);

-- hash worker: oldest UPLOADED first
CREATE INDEX IF NOT EXISTS idx_upload_intent_status_created_at ON upload_intent(status, created_at);
-- recovery sweep: stale HASHING rows
CREATE INDEX IF NOT EXISTS idx_upload_intent_status_claimed_at ON upload_intent(status, claimed_at);
CREATE INDEX IF NOT EXISTS idx_upload_intent_expires_at ON upload_intent(expires_at);
