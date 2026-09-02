package db

import (
	"database/sql"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

func Open(path string) (*sql.DB, error) {
	if path != ":memory:" {
		if dir := filepath.Dir(path); dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
		}
	}

	conn, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
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
