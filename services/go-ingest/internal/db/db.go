package db

import (
	"database/sql"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

func Open(path string) (*sql.DB, error) {
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
	}

	conn, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// Serialize all access through a single connection. This registry is a
	// single-writer store at this scale, and SetMaxOpenConns(1) both avoids
	// SQLITE_BUSY collisions between the outbox poller and confirm handlers
	// and sidesteps the classic ":memory:" footgun where each pooled
	// connection would otherwise see its own fresh, empty database.
	conn.SetMaxOpenConns(1)
	if _, err := conn.Exec(`
		CREATE TABLE IF NOT EXISTS documents (
			hash_id      TEXT PRIMARY KEY,
			object_path  TEXT NOT NULL,
			status       TEXT NOT NULL,
			published_at TIMESTAMP,
			confirmed_at TIMESTAMP NOT NULL
		)
	`); err != nil {
		return nil, err
	}
	return conn, nil
}
