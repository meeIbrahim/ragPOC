// Package testutil holds helpers shared by the center service's tests.
package testutil

import (
	"database/sql"
	"os"
	"path/filepath"
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
// from an embedded FS, so tests read the same files from disk. The module root is found from
// the working directory (go test runs in the package directory) because runtime.Caller paths
// are rewritten by -trimpath.
func migrationFiles(t testing.TB) []string {
	t.Helper()
	dir := filepath.Join(moduleRoot(t), "migrations")
	files, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no migrations found in %s (err %v)", dir, err)
	}
	sort.Strings(files)
	return files
}

func moduleRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test's working directory")
		}
		dir = parent
	}
}
