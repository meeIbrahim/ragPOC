package sqlite

import (
	"center-service/internal/config"
	"database/sql"
	"fmt"
	"path/filepath"

	_ "modernc.org/sqlite"
)

func Open(cfg *config.Config) (*sql.DB, error) {
	path := filepath.Join(cfg.Go.SQLiteDir, "center.db")
	if path == "" {
		return nil, fmt.Errorf("Empty DB Path")
	}
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}
