CREATE TABLE IF NOT EXISTS upload_intents (
    id           TEXT PRIMARY KEY NOT NULL,
    file_name    TEXT NOT NULL,
    object_path  TEXT,
    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at   TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status       TEXT NOT NULL DEFAULT 'waiting' CHECK (status IN (
        'unknown', 'waiting', 'uploaded', 'processing', 'completed',
        'duplicate', 'failed', 'expired', 'verification_failed'
    ))
);

CREATE INDEX IF NOT EXISTS idx_upload_intents_status ON upload_intents(status);
CREATE INDEX IF NOT EXISTS idx_upload_intents_expires_at ON upload_intents(expires_at);
