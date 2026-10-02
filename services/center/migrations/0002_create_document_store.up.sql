CREATE TABLE IF NOT EXISTS document_store (
    hash             TEXT PRIMARY KEY NOT NULL,
    perm_bucket_path TEXT NOT NULL,
    file_name        TEXT NOT NULL,
    size             INTEGER NOT NULL,
    status           TEXT NOT NULL DEFAULT 'PROCESSING' CHECK (status IN (
        'PROCESSING', 'INDEXED', 'FAILED'
    )),
    retries          INTEGER NOT NULL DEFAULT 0,
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_document_store_status ON document_store(status);
