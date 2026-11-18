package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	"center-service/internal/domain"
	"center-service/internal/store/sqlite"
)

const (
	hA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	hB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// listIntent inserts an upload with its own name, so a test can tell which row a filename came
// from. hash is nil for an unhashed upload.
func listIntent(t *testing.T, db *sql.DB, id string, status domain.IntentState, hash any, createdAt time.Time) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO upload_intent (uuid, file_name, hash, status, created_at, expires_at) VALUES (?,?,?,?,?,?)`,
		id, "intent-"+id+".txt", hash, status.String(), createdAt, createdAt.Add(time.Hour))
	if err != nil {
		t.Fatalf("insert intent %s: %v", id, err)
	}
}

func setReason(t *testing.T, db *sql.DB, h, reason string) {
	t.Helper()
	if _, err := db.Exec(`UPDATE document_store SET failure_reason = ? WHERE hash = ?`, reason, h); err != nil {
		t.Fatal(err)
	}
}

func list(t *testing.T, s *sqlite.Store, limit int) ([]domain.DocumentEntry, bool) {
	t.Helper()
	entries, truncated, err := s.ListDocuments(context.Background(), limit)
	if err != nil {
		t.Fatalf("ListDocuments: %v", err)
	}
	return entries, truncated
}

func ids(entries []domain.DocumentEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.ID
	}
	return out
}

func at(minute int) time.Time { return created.Add(time.Duration(minute) * time.Minute) }

func TestListDocumentsEmptyIsNonNil(t *testing.T) {
	s, _ := newStore(t)
	entries, truncated := list(t, s, 10)
	if entries == nil || len(entries) != 0 || truncated {
		t.Errorf("empty list = %#v, truncated=%v; want an empty non-nil slice so the JSON is [] not null", entries, truncated)
	}
}

func TestListDocumentsOneHashedUpload(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hA, domain.DocumentIndexed, 0)
	listIntent(t, db, "u1", domain.IntentHashed, hA, at(1))

	entries, _ := list(t, s, 10)
	want := domain.DocumentEntry{ID: "u1", UploadIDs: []string{"u1"}, FileName: "a.txt", Status: domain.DocumentIndexed}
	if len(entries) != 1 {
		t.Fatalf("entries = %+v; want one", entries)
	}
	e := entries[0]
	// FileName comes from document_store (A1): it is the name the answer's sources show.
	if e.ID != want.ID || !slices.Equal(e.UploadIDs, want.UploadIDs) || e.FileName != want.FileName ||
		e.Status != want.Status || e.FailureReason != "" {
		t.Errorf("entry = %+v; want %+v", e, want)
	}
	// modernc needs the declared column type to scan into time.Time; a CTE or window function
	// can lose it.
	if !e.CreatedAt.Equal(at(1)) {
		t.Errorf("created_at = %v; want %v scanned back as a time", e.CreatedAt, at(1))
	}
}

func TestListDocumentsMergesDuplicateUploads(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hA, domain.DocumentProcessing, 0)
	listIntent(t, db, "u-new", domain.IntentHashed, hA, at(5))
	listIntent(t, db, "u-old", domain.IntentHashed, hA, at(1))
	listIntent(t, db, "u-mid", domain.IntentHashed, hA, at(3))

	entries, _ := list(t, s, 10)
	if len(entries) != 1 {
		t.Fatalf("entries = %+v; want one: duplicate uploads of one hash are one document", entries)
	}
	e := entries[0]
	if e.ID != "u-old" || !e.CreatedAt.Equal(at(1)) {
		t.Errorf("id/created_at = %s/%v; want u-old/%v: the oldest upload names the entry, so it never changes", e.ID, e.CreatedAt, at(1))
	}
	if want := []string{"u-old", "u-mid", "u-new"}; !slices.Equal(e.UploadIDs, want) {
		t.Errorf("upload_ids = %v; want %v, oldest first", e.UploadIDs, want)
	}
}

// Two uploads of one hash created in the same instant: the uuid decides, the same way every time.
func TestListDocumentsMergeTieBreaksOnUploadID(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hA, domain.DocumentIndexed, 0)
	listIntent(t, db, "u2", domain.IntentHashed, hA, at(1))
	listIntent(t, db, "u1", domain.IntentHashed, hA, at(1))

	entries, _ := list(t, s, 10)
	if len(entries) != 1 || entries[0].ID != "u1" || !slices.Equal(entries[0].UploadIDs, []string{"u1", "u2"}) {
		t.Errorf("entries = %+v; want one entry u1 with upload_ids [u1 u2]", entries)
	}
}

func TestListDocumentsPendingUploads(t *testing.T) {
	s, db := newStore(t)
	listIntent(t, db, "uploaded", domain.IntentUploaded, nil, at(1))
	listIntent(t, db, "hashing", domain.IntentHashing, nil, at(2))
	listIntent(t, db, "failed", domain.IntentFailed, nil, at(3))
	listIntent(t, db, "waiting", domain.IntentWaiting, nil, at(4))
	listIntent(t, db, "expired", domain.IntentExpired, nil, at(5))
	listIntent(t, db, "hashed-no-hash", domain.IntentHashed, nil, at(6)) // the poll says PROCESSING too

	entries, _ := list(t, s, 10)
	got := map[string]domain.DocumentEntry{}
	for _, e := range entries {
		got[e.ID] = e
	}
	for _, hidden := range []string{"waiting", "expired"} {
		if _, ok := got[hidden]; ok {
			t.Errorf("%s upload listed; want it hidden: it never reached the server or was abandoned", hidden)
		}
	}
	want := map[string]struct {
		status domain.DocumentStatus
		reason string
	}{
		"uploaded":       {domain.DocumentProcessing, ""},
		"hashing":        {domain.DocumentProcessing, ""},
		"hashed-no-hash": {domain.DocumentProcessing, ""},
		"failed":         {domain.DocumentFailed, domain.ReasonUploadFailed},
	}
	for id, w := range want {
		e, ok := got[id]
		if !ok {
			t.Errorf("%s upload missing from the list", id)
			continue
		}
		if e.Status != w.status || e.FailureReason != w.reason {
			t.Errorf("%s = %s/%q; want %s/%q", id, e.Status, e.FailureReason, w.status, w.reason)
		}
		// A pending upload has no hash, so its only upload id is itself.
		if !slices.Equal(e.UploadIDs, []string{id}) {
			t.Errorf("%s upload_ids = %v; want exactly [%s]", id, e.UploadIDs, id)
		}
		if e.FileName != "intent-"+id+".txt" {
			t.Errorf("%s filename = %q; want the intent's name (no document row yet)", id, e.FileName)
		}
	}
}

func TestListDocumentsFailureReasons(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hA, domain.DocumentFailed, 0)
	setReason(t, db, hA, "EMPTY_TEXT")
	insertDoc(t, db, hB, domain.DocumentFailed, 2) // failed before migration 0004: NULL reason
	listIntent(t, db, "with-reason", domain.IntentHashed, hA, at(1))
	listIntent(t, db, "null-reason", domain.IntentHashed, hB, at(2))

	got := map[string]domain.DocumentEntry{}
	entries, _ := list(t, s, 10)
	for _, e := range entries {
		got[e.ID] = e
	}
	if e := got["with-reason"]; e.Status != domain.DocumentFailed || e.FailureReason != "EMPTY_TEXT" {
		t.Errorf("with-reason = %s/%q; want FAILED/EMPTY_TEXT", e.Status, e.FailureReason)
	}
	if e := got["null-reason"]; e.Status != domain.DocumentFailed || e.FailureReason != "" {
		t.Errorf("null-reason = %s/%q; want FAILED with no reason", e.Status, e.FailureReason)
	}
}

// Newest first, hashed and pending entries interleaved; equal created_at falls back to id DESC.
func TestListDocumentsOrder(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hA, domain.DocumentIndexed, 0)
	insertDoc(t, db, hB, domain.DocumentIndexed, 0)
	listIntent(t, db, "a-old", domain.IntentHashed, hA, at(1))
	listIntent(t, db, "a-dup", domain.IntentHashed, hA, at(9)) // must not move hA's entry
	listIntent(t, db, "p-1", domain.IntentUploaded, nil, at(2))
	listIntent(t, db, "b-1", domain.IntentHashed, hB, at(3))
	listIntent(t, db, "tie-x", domain.IntentUploaded, nil, at(4))
	listIntent(t, db, "tie-y", domain.IntentHashing, nil, at(4))

	entries, _ := list(t, s, 10)
	if want := []string{"tie-y", "tie-x", "b-1", "p-1", "a-old"}; !slices.Equal(ids(entries), want) {
		t.Errorf("order = %v; want %v (created_at DESC, id DESC; an entry's time is its oldest upload's)", ids(entries), want)
	}
}

func TestListDocumentsCap(t *testing.T) {
	const limit = 4
	for _, n := range []int{limit - 1, limit, limit + 1} {
		t.Run(fmt.Sprintf("%d entries", n), func(t *testing.T) {
			s, db := newStore(t)
			// Alternate hashed (with a duplicate, so it has two rows) and pending entries.
			for i := range n {
				id := fmt.Sprintf("e%02d", i)
				if i%2 == 0 {
					h := fmt.Sprintf("%064d", i)
					insertDoc(t, db, h, domain.DocumentIndexed, 0)
					listIntent(t, db, id, domain.IntentHashed, h, at(i))
					listIntent(t, db, id+"-dup", domain.IntentHashed, h, at(100+i))
				} else {
					listIntent(t, db, id, domain.IntentUploaded, nil, at(i))
				}
			}

			entries, truncated := list(t, s, limit)
			wantLen := min(n, limit)
			if len(entries) != wantLen || truncated != (n > limit) {
				t.Fatalf("len=%d truncated=%v; want len=%d truncated=%v", len(entries), truncated, wantLen, n > limit)
			}
			// The newest entries are kept; the cut drops the oldest.
			if entries[0].ID != fmt.Sprintf("e%02d", n-1) {
				t.Errorf("first entry = %s; want the newest, e%02d", entries[0].ID, n-1)
			}
			for _, e := range entries {
				if wantIDs := 1 + boolInt(e.Status == domain.DocumentIndexed); len(e.UploadIDs) != wantIDs {
					t.Errorf("%s upload_ids = %v; want %d ids: the cap counts entries, not rows", e.ID, e.UploadIDs, wantIDs)
				}
			}
		})
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
