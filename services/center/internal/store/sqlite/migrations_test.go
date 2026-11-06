package sqlite_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"center-service/internal/testutil"
)

var tables = []string{"upload_intent", "document_store", "outbox"}

func applyMigrations(t *testing.T, db *sql.DB, direction string) {
	t.Helper()
	for _, file := range testutil.MigrationFiles(t, direction) {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(string(body)); err != nil {
			t.Fatalf("%s: %v", filepath.Base(file), err)
		}
	}
}

func schemaObjects(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Every down migration must undo its up migration completely, in reverse order, so a rollback
// followed by a re-apply leaves the same schema.
func TestMigrationsGoDownAndUpAgain(t *testing.T) {
	db := testutil.NewSQLite(t) // every up migration applied
	full := schemaObjects(t, db)
	if full == 0 {
		t.Fatal("no schema after the up migrations")
	}

	applyMigrations(t, db, "down")
	if n := schemaObjects(t, db); n != 0 {
		t.Errorf("%d tables or indexes left after every down migration; want 0: a down migration misses something its up created", n)
	}

	applyMigrations(t, db, "up")
	if n := schemaObjects(t, db); n != full {
		t.Errorf("%d tables and indexes after down then up; want %d, as after the first up", n, full)
	}
}

func TestStatusCheckConstraintsRejectUnknownStatuses(t *testing.T) {
	db := testutil.NewSQLite(t)
	insertDoc(t, db, hash, "PROCESSING", 0)

	cases := map[string]string{
		"upload_intent":  `INSERT INTO upload_intent (uuid, file_name, status, expires_at) VALUES ('i', 'f', 'BOGUS', '2030-01-01')`,
		"document_store": `INSERT INTO document_store (hash, perm_bucket_path, file_name, size, status) VALUES ('h2', 'p', 'f', 1, 'BOGUS')`,
		"outbox":         `INSERT INTO outbox (hash, status) VALUES ('` + hash + `', 'BOGUS')`,
	}
	for _, table := range tables {
		if _, err := db.Exec(cases[table]); err == nil {
			t.Errorf("%s accepted status BOGUS; the CHECK constraint must reject a status outside its state machine", table)
		}
	}
}
