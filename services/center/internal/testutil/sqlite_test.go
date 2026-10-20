package testutil

import (
	"testing"
)

func TestNewSQLiteAppliesAllMigrations(t *testing.T) {
	db := NewSQLite(t)
	for _, table := range []string{"upload_intent", "document_store", "outbox"} {
		var name string
		err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name)
		if err != nil {
			t.Errorf("table %s missing: %v", table, err)
		}
	}
}

// The helper must open the database through sqlite.Open, so tests run under the production
// pragmas and a connection-level difference cannot hide a bug.
func TestNewSQLiteUsesProductionSettings(t *testing.T) {
	db := NewSQLite(t)

	if got := db.Stats().MaxOpenConnections; got != 1 {
		t.Errorf("MaxOpenConnections = %d, want 1", got)
	}
	var fk int
	if err := db.QueryRow(`PRAGMA foreign_keys`).Scan(&fk); err != nil || fk != 1 {
		t.Errorf("foreign_keys = %d, err %v, want 1", fk, err)
	}
	var mode string
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
		t.Errorf("journal_mode = %q, err %v, want wal", mode, err)
	}
	var busy int
	if err := db.QueryRow(`PRAGMA busy_timeout`).Scan(&busy); err != nil || busy != 5000 {
		t.Errorf("busy_timeout = %d, err %v, want 5000", busy, err)
	}
}

func TestNewSQLiteEnforcesSchemaConstraints(t *testing.T) {
	db := NewSQLite(t)

	if _, err := db.Exec(`INSERT INTO document_store (hash, perm_bucket_path, file_name, size, status) VALUES ('h', 'p', 'f', 1, 'BOGUS')`); err == nil {
		t.Error("CHECK constraint on document_store.status not enforced")
	}
	// foreign_keys=ON: an outbox row for an unknown hash must be rejected.
	if _, err := db.Exec(`INSERT INTO outbox (hash) VALUES ('missing')`); err == nil {
		t.Error("outbox.hash foreign key not enforced")
	}
}

func TestNewSQLiteIsIsolatedPerCall(t *testing.T) {
	a, b := NewSQLite(t), NewSQLite(t)
	if _, err := a.Exec(`INSERT INTO document_store (hash, perm_bucket_path, file_name, size) VALUES ('h', 'p', 'f', 1)`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := b.QueryRow(`SELECT COUNT(*) FROM document_store`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("second DB sees %d rows (err %v), want 0", n, err)
	}
}
