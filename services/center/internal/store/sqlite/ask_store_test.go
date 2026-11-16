package sqlite_test

import (
	"context"
	"database/sql"
	"testing"

	"center-service/internal/domain"
)

func insertIntent(t *testing.T, db *sql.DB, id, status string, hash any) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO upload_intent (uuid, file_name, hash, status, expires_at) VALUES (?, 'f.txt', ?, ?, ?)`,
		id, hash, status, now)
	if err != nil {
		t.Fatal(err)
	}
}

func TestResolveUploads(t *testing.T) {
	s, db := newStore(t)
	const (
		hIndexed    = "1111111111111111111111111111111111111111111111111111111111111111"
		hProcessing = "2222222222222222222222222222222222222222222222222222222222222222"
	)
	insertDoc(t, db, hIndexed, domain.DocumentIndexed, 0)
	insertDoc(t, db, hProcessing, domain.DocumentProcessing, 0)
	insertIntent(t, db, "up-indexed", "HASHED", hIndexed)
	insertIntent(t, db, "up-indexed-dup", "HASHED", hIndexed)
	insertIntent(t, db, "up-processing", "HASHED", hProcessing)
	insertIntent(t, db, "up-unhashed", "UPLOADED", nil)

	refs, err := s.ResolveUploads(context.Background(),
		[]string{"up-indexed", "up-indexed-dup", "up-processing", "up-unhashed", "up-unknown"})
	if err != nil {
		t.Fatalf("ResolveUploads: %v", err)
	}
	got := map[string]domain.UploadRef{}
	for _, r := range refs {
		if _, dup := got[r.UploadID]; dup {
			t.Fatalf("upload %s returned twice", r.UploadID)
		}
		got[r.UploadID] = r
	}
	want := map[string]domain.UploadRef{
		"up-indexed":     {UploadID: "up-indexed", Hash: hIndexed, DocStatus: domain.DocumentIndexed},
		"up-indexed-dup": {UploadID: "up-indexed-dup", Hash: hIndexed, DocStatus: domain.DocumentIndexed},
		"up-processing":  {UploadID: "up-processing", Hash: hProcessing, DocStatus: domain.DocumentProcessing},
		"up-unhashed":    {UploadID: "up-unhashed"},
	}
	if len(got) != len(want) {
		t.Errorf("got %d refs %v, want %d: an unknown id must be absent, every known id present", len(got), refs, len(want))
	}
	for id, w := range want {
		if g, ok := got[id]; !ok {
			t.Errorf("known upload %s missing: the caller would report it as unknown", id)
		} else if g != w {
			t.Errorf("upload %s = %+v, want %+v", id, g, w)
		}
	}
	if _, ok := got["up-unknown"]; ok {
		t.Error("unknown upload id returned: it must be absent so the caller can reject it")
	}
}

func TestResolveUploadsEmptyIsNoOp(t *testing.T) {
	s, db := newStore(t)
	// A closed database proves no query runs for an empty selection.
	db.Close()
	refs, err := s.ResolveUploads(context.Background(), nil)
	if err != nil || len(refs) != 0 {
		t.Fatalf("ResolveUploads(nil) = %v, %v; want nothing and no query", refs, err)
	}
}

func TestResolveUploadsReportsStoreError(t *testing.T) {
	s, db := newStore(t)
	db.Close()
	if _, err := s.ResolveUploads(context.Background(), []string{"x"}); err == nil {
		t.Fatal("ResolveUploads on a closed database returned no error: the handler must answer 500, not treat ids as unknown")
	}
}
