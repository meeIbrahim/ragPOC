package sqlite_test

import (
	"database/sql"
	"fmt"
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

	// %[1]s is the status; the same insert with a valid status must succeed, so a rejection can
	// only come from the CHECK constraint.
	cases := map[string]struct{ insert, valid string }{
		"upload_intent":  {`INSERT INTO upload_intent (uuid, file_name, status, expires_at) VALUES ('i-%[1]s', 'f', '%[1]s', '2030-01-01')`, "WAITING"},
		"document_store": {`INSERT INTO document_store (hash, perm_bucket_path, file_name, size, status) VALUES ('h-%[1]s', 'p', 'f', 1, '%[1]s')`, "INDEXED"},
		"outbox":         {`INSERT INTO outbox (hash, status) VALUES ('` + hash + `', '%[1]s')`, "PUBLISHED"},
	}
	for _, table := range tables {
		c := cases[table]
		if _, err := db.Exec(fmt.Sprintf(c.insert, c.valid)); err != nil {
			t.Fatalf("%s rejected the valid status %s: %v; the insert itself is broken, so the BOGUS case proves nothing", table, c.valid, err)
		}
		if _, err := db.Exec(fmt.Sprintf(c.insert, "BOGUS")); err == nil {
			t.Errorf("%s accepted status BOGUS; the CHECK constraint must reject a status outside its state machine", table)
		}
	}
}

func execMigration(t *testing.T, db *sql.DB, name string) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", name))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(body)); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

func hasColumn(t *testing.T, db *sql.DB, table, column string) bool {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`, table, column).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

// 0004 runs on databases that already hold documents, so both directions must keep the rows.
func TestFailureReasonMigrationKeepsExistingRows(t *testing.T) {
	db := testutil.NewSQLite(t)
	execMigration(t, db, "0004_add_document_failure_reason.down.sql")
	insertDoc(t, db, hash, "FAILED", 3)

	execMigration(t, db, "0004_add_document_failure_reason.up.sql")
	var reason sql.NullString
	if err := db.QueryRow(`SELECT failure_reason FROM document_store WHERE hash = ?`, hash).Scan(&reason); err != nil {
		t.Fatalf("existing row after 0004 up: %v", err)
	}
	if reason.Valid {
		t.Errorf("failure_reason = %q on a row that existed before 0004; want NULL: the migration must not invent a reason", reason.String)
	}

	if _, err := db.Exec(`UPDATE document_store SET failure_reason = 'CORRUPT_FILE' WHERE hash = ?`, hash); err != nil {
		t.Fatal(err)
	}
	execMigration(t, db, "0004_add_document_failure_reason.down.sql")
	if hasColumn(t, db, "document_store", "failure_reason") {
		t.Error("failure_reason still exists after 0004 down")
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM document_store WHERE hash = ?`, hash).Scan(&status); err != nil || status != "FAILED" {
		t.Errorf("row after 0004 down = %q, %v; want FAILED: dropping the column must keep the document", status, err)
	}
}
