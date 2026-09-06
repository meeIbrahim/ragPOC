CREATE TABLE IF NOT EXISTS documents (
    content_hash TEXT PRIMARY KEY NOT NULL,
    object_path  TEXT NOT NULL,
    file_name    TEXT NOT NULL,
    size         INTEGER NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
