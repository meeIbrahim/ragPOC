CREATE TABLE IF NOT EXISTS ingestion_processes (
    content_hash TEXT PRIMARY KEY NOT NULL REFERENCES documents(content_hash) ON DELETE CASCADE,
    started_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    state        TEXT NOT NULL DEFAULT 'idle' CHECK (state IN (
        'unknown', 'idle', 'processing', 'complete', 'error'
    ))
);

CREATE INDEX IF NOT EXISTS idx_ingestion_processes_state ON ingestion_processes(state);
