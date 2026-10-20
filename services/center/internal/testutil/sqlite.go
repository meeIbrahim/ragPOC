// Package testutil holds helpers shared by the center service's tests.
package testutil

import (
	"database/sql"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"center-service/internal/config"
	"center-service/internal/store/sqlite"
)

// NewSQLite returns a fresh temp-file database with every migrations/*.up.sql applied. It is
// opened through sqlite.Open, so it has the production pragmas (WAL, busy_timeout,
// foreign_keys) and the single-connection limit. It is closed when the test ends.
//
// Tests of package store/sqlite itself must use an external test package (sqlite_test), because
// this package imports store/sqlite.
func NewSQLite(t testing.TB) *sql.DB {
	t.Helper()

	cfg := &config.Config{Go: config.GoConfig{SQLiteDir: t.TempDir()}}
	db, err := sqlite.Open(cfg)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	for _, file := range migrationFiles(t) {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		if _, err := db.Exec(string(body)); err != nil {
			t.Fatalf("apply %s: %v", filepath.Base(file), err)
		}
	}
	return db
}

// migrationFiles returns the up migrations in version order. The migrate binary reads them
// from an embedded FS, so tests read the same files from disk.
func migrationFiles(t testing.TB) []string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate testutil source")
	}
	dir := filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations")
	files, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no migrations found in %s (err %v)", dir, err)
	}
	sort.Strings(files)
	return files
}
