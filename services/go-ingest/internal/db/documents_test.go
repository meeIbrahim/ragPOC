package db

import "testing"

func TestInsertGetAndOutboxSweep(t *testing.T) {
	conn, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	if _, err := GetByHash(conn, "missing"); err != nil {
		t.Fatalf("GetByHash on empty table: %v", err)
	}
	if got, _ := GetByHash(conn, "missing"); got != nil {
		t.Fatalf("GetByHash on empty table = %+v, want nil", got)
	}

	if err := Insert(conn, "abc123", "objects/abc.pdf"); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	doc, err := GetByHash(conn, "abc123")
	if err != nil {
		t.Fatalf("GetByHash: %v", err)
	}
	if doc == nil || doc.ObjectPath != "objects/abc.pdf" {
		t.Fatalf("GetByHash = %+v, want object_path=objects/abc.pdf", doc)
	}
	if doc.PublishedAt.Valid {
		t.Fatalf("PublishedAt should start unset, got %v", doc.PublishedAt)
	}

	unpublished, err := UnpublishedHashes(conn)
	if err != nil {
		t.Fatalf("UnpublishedHashes: %v", err)
	}
	if len(unpublished) != 1 || unpublished[0].HashID != "abc123" {
		t.Fatalf("UnpublishedHashes = %+v, want one row for abc123", unpublished)
	}

	if err := MarkPublished(conn, "abc123"); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}

	unpublished, err = UnpublishedHashes(conn)
	if err != nil {
		t.Fatalf("UnpublishedHashes after publish: %v", err)
	}
	if len(unpublished) != 0 {
		t.Fatalf("UnpublishedHashes after publish = %+v, want none", unpublished)
	}
}

func TestInsertIfAbsent(t *testing.T) {
	conn, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	inserted, err := InsertIfAbsent(conn, "abc123", "objects/abc.pdf")
	if err != nil {
		t.Fatalf("InsertIfAbsent (first): %v", err)
	}
	if !inserted {
		t.Fatalf("InsertIfAbsent (first) = false, want true for a new hash")
	}

	doc, err := GetByHash(conn, "abc123")
	if err != nil || doc == nil {
		t.Fatalf("GetByHash after InsertIfAbsent: doc=%+v err=%v", doc, err)
	}

	inserted, err = InsertIfAbsent(conn, "abc123", "objects/dup.pdf")
	if err != nil {
		t.Fatalf("InsertIfAbsent (duplicate): %v", err)
	}
	if inserted {
		t.Fatalf("InsertIfAbsent (duplicate) = true, want false for an existing hash")
	}

	// The original row must be untouched by the losing insert attempt.
	doc, err = GetByHash(conn, "abc123")
	if err != nil || doc == nil || doc.ObjectPath != "objects/abc.pdf" {
		t.Fatalf("GetByHash after duplicate InsertIfAbsent = %+v, err=%v, want unchanged object_path=objects/abc.pdf", doc, err)
	}
}
