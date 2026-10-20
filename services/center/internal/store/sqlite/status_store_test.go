package sqlite_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"center-service/internal/domain"
	"center-service/internal/store/sqlite"
	"center-service/internal/testutil"
)

const hash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"

var (
	created = time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	now     = time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
)

func newStore(t *testing.T) (*sqlite.Store, *sql.DB) {
	t.Helper()
	db := testutil.NewSQLite(t)
	return sqlite.NewStore(db), db
}

func insertDoc(t *testing.T, db *sql.DB, h string, status domain.DocumentStatus, retries int) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO document_store (hash, perm_bucket_path, file_name, size, status, retries, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?)`, h, "ba/78/"+h, "a.txt", 3, status, retries, created, created)
	if err != nil {
		t.Fatal(err)
	}
}

func getDoc(t *testing.T, s *sqlite.Store, h string) *domain.Document {
	t.Helper()
	d, err := s.GetDocumentByHash(context.Background(), h)
	if err != nil || d == nil {
		t.Fatalf("document %s: %v, %v", h, d, err)
	}
	return d
}

func outboxRows(t *testing.T, db *sql.DB, h string) (total, pending int) {
	t.Helper()
	if err := db.QueryRow(`SELECT COUNT(*), COALESCE(SUM(status = 'PENDING'), 0) FROM outbox WHERE hash = ?`, h).Scan(&total, &pending); err != nil {
		t.Fatal(err)
	}
	return total, pending
}

func assertDoc(t *testing.T, s *sqlite.Store, status domain.DocumentStatus, retries int, touched bool) {
	t.Helper()
	d := getDoc(t, s, hash)
	if d.Status != status || d.Retries != retries {
		t.Errorf("document = %s/retries=%d, want %s/retries=%d", d.Status, d.Retries, status, retries)
	}
	if touched && !d.UpdatedAt.Equal(now) {
		t.Errorf("updated_at = %v, want %v (every write sets it)", d.UpdatedAt, now)
	}
	if !touched && !d.UpdatedAt.Equal(created) {
		t.Errorf("updated_at = %v, want it untouched at %v", d.UpdatedAt, created)
	}
}

// ---- ApplyIndexed

func TestApplyIndexedMarksProcessingDocument(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hash, domain.DocumentProcessing, 0)

	applied, err := s.ApplyIndexed(context.Background(), hash, 0, now)
	if err != nil || !applied {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	assertDoc(t, s, domain.DocumentIndexed, 0, true)
}

func TestApplyIndexedDuplicateIsNoOp(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hash, domain.DocumentProcessing, 0)
	if _, err := s.ApplyIndexed(context.Background(), hash, 0, now); err != nil {
		t.Fatal(err)
	}

	later := now.Add(time.Hour)
	applied, err := s.ApplyIndexed(context.Background(), hash, 0, later)
	if err != nil || applied {
		t.Fatalf("duplicate: applied=%v err=%v, want false/nil", applied, err)
	}
	assertDoc(t, s, domain.DocumentIndexed, 0, true) // updated_at still the first write
}

func TestApplyIndexedStaleAttemptIsRejected(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hash, domain.DocumentProcessing, 1) // a retry already started

	applied, err := s.ApplyIndexed(context.Background(), hash, 0, now)
	if err != nil || applied {
		t.Fatalf("applied=%v err=%v, want false/nil for attempt 0 against retries=1", applied, err)
	}
	assertDoc(t, s, domain.DocumentProcessing, 1, false)
}

// PRD 4.3: INDEXED is guarded by status='PROCESSING', so a late INDEXED after the document
// was marked FAILED is discarded and the document stays FAILED.
func TestApplyIndexedOnFailedDocumentIsRejected(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hash, domain.DocumentFailed, 2)

	applied, err := s.ApplyIndexed(context.Background(), hash, 2, now)
	if err != nil || applied {
		t.Fatalf("applied=%v err=%v, want false/nil", applied, err)
	}
	assertDoc(t, s, domain.DocumentFailed, 2, false)
}

func TestApplyIndexedUnknownHash(t *testing.T) {
	s, _ := newStore(t)
	applied, err := s.ApplyIndexed(context.Background(), "unknown", 0, now)
	if err != nil || applied {
		t.Fatalf("applied=%v err=%v, want false/nil", applied, err)
	}
}

// retries is never reset: INDEXED after earlier retries keeps the count.
func TestApplyIndexedKeepsRetries(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hash, domain.DocumentProcessing, 2)
	if applied, err := s.ApplyIndexed(context.Background(), hash, 2, now); err != nil || !applied {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	assertDoc(t, s, domain.DocumentIndexed, 2, true)
}

// ---- ApplyFailed

func TestApplyFailedRetryableQueuesNextAttempt(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hash, domain.DocumentProcessing, 0)

	out, err := s.ApplyFailed(context.Background(), hash, 0, true, 3, now)
	if err != nil || out != domain.FailedRetried {
		t.Fatalf("outcome=%v err=%v, want Retried", out, err)
	}
	assertDoc(t, s, domain.DocumentProcessing, 1, true)
	if total, pending := outboxRows(t, db, hash); total != 1 || pending != 1 {
		t.Errorf("outbox rows total=%d pending=%d, want exactly one PENDING row", total, pending)
	}
}

func TestApplyFailedRetryableAtLimitMarksFailed(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hash, domain.DocumentProcessing, 2) // attempt 2 of max 3 is the last

	out, err := s.ApplyFailed(context.Background(), hash, 2, true, 3, now)
	if err != nil || out != domain.FailedExhausted {
		t.Fatalf("outcome=%v err=%v, want Exhausted", out, err)
	}
	assertDoc(t, s, domain.DocumentFailed, 2, true)
	if total, _ := outboxRows(t, db, hash); total != 0 {
		t.Errorf("outbox rows = %d, want none", total)
	}
}

func TestApplyFailedPermanentMarksFailedWithoutRetry(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hash, domain.DocumentProcessing, 0)

	out, err := s.ApplyFailed(context.Background(), hash, 0, false, 3, now)
	if err != nil || out != domain.FailedExhausted {
		t.Fatalf("outcome=%v err=%v, want Exhausted", out, err)
	}
	assertDoc(t, s, domain.DocumentFailed, 0, true) // permanent failures never consume retries
	if total, _ := outboxRows(t, db, hash); total != 0 {
		t.Errorf("outbox rows = %d, want none", total)
	}
}

func TestApplyFailedMaxAttemptsOneNeverRetries(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hash, domain.DocumentProcessing, 0)
	out, err := s.ApplyFailed(context.Background(), hash, 0, true, 1, now)
	if err != nil || out != domain.FailedExhausted {
		t.Fatalf("outcome=%v err=%v, want Exhausted when max_attempts=1", out, err)
	}
}

func TestApplyFailedDuplicateIsStale(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hash, domain.DocumentProcessing, 0)
	if _, err := s.ApplyFailed(context.Background(), hash, 0, true, 3, now); err != nil {
		t.Fatal(err)
	}

	out, err := s.ApplyFailed(context.Background(), hash, 0, true, 3, now.Add(time.Hour))
	if err != nil || out != domain.FailedStale {
		t.Fatalf("outcome=%v err=%v, want Stale for the redelivered FAILED(0)", out, err)
	}
	assertDoc(t, s, domain.DocumentProcessing, 1, true)
	if total, _ := outboxRows(t, db, hash); total != 1 {
		t.Errorf("outbox rows = %d, want still 1 (a duplicate must not queue another attempt)", total)
	}
}

func TestApplyFailedAfterIndexedLeavesIndexed(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hash, domain.DocumentProcessing, 0)
	if _, err := s.ApplyIndexed(context.Background(), hash, 0, now); err != nil {
		t.Fatal(err)
	}

	out, err := s.ApplyFailed(context.Background(), hash, 0, true, 3, now.Add(time.Hour))
	if err != nil || out != domain.FailedStale {
		t.Fatalf("outcome=%v err=%v, want Stale", out, err)
	}
	assertDoc(t, s, domain.DocumentIndexed, 0, true)
	if total, _ := outboxRows(t, db, hash); total != 0 {
		t.Errorf("outbox rows = %d, want none", total)
	}
}

func TestApplyFailedUnknownHashIsStale(t *testing.T) {
	s, db := newStore(t)
	out, err := s.ApplyFailed(context.Background(), "unknown", 0, true, 3, now)
	if err != nil || out != domain.FailedStale {
		t.Fatalf("outcome=%v err=%v, want Stale", out, err)
	}
	if total, _ := outboxRows(t, db, "unknown"); total != 0 {
		t.Errorf("outbox rows = %d", total)
	}
}

// The document update and the outbox insert are one transaction: if the insert fails the
// retries bump must not survive, or the document would claim a retry no event will carry.
func TestApplyFailedRollsBackWhenOutboxInsertFails(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hash, domain.DocumentProcessing, 0)
	if _, err := db.Exec(`DROP TABLE outbox`); err != nil {
		t.Fatal(err)
	}

	out, err := s.ApplyFailed(context.Background(), hash, 0, true, 3, now)
	if err == nil {
		t.Fatalf("outcome=%v, want the outbox error", out)
	}
	assertDoc(t, s, domain.DocumentProcessing, 0, false)
}

// Two consumers handling the same redelivered FAILED(0) race: exactly one wins and exactly
// one outbox row exists afterwards.
func TestApplyFailedConcurrentDuplicatesApplyOnce(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hash, domain.DocumentProcessing, 0)

	const workers = 8
	outcomes := make(chan domain.FailedOutcome, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := s.ApplyFailed(context.Background(), hash, 0, true, 3, now)
			if err != nil {
				t.Error(err)
			}
			outcomes <- out
		}()
	}
	wg.Wait()
	close(outcomes)

	retried := 0
	for out := range outcomes {
		if out == domain.FailedRetried {
			retried++
		}
	}
	if retried != 1 {
		t.Errorf("%d goroutines reported Retried, want exactly 1", retried)
	}
	if total, _ := outboxRows(t, db, hash); total != 1 {
		t.Errorf("outbox rows = %d, want 1", total)
	}
	assertDoc(t, s, domain.DocumentProcessing, 1, true)
}

// The full PRD acceptance path for failed-retryable with max_attempts=3: 3 outbox rows in
// total (one from ingestion + two retries), retries=2, FAILED.
func TestApplyFailedFullRetryLadder(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hash, domain.DocumentProcessing, 0)
	if _, err := db.Exec(`INSERT INTO outbox (hash, status, created_at) VALUES (?, 'PUBLISHED', ?)`, hash, created); err != nil {
		t.Fatal(err)
	}

	want := []domain.FailedOutcome{domain.FailedRetried, domain.FailedRetried, domain.FailedExhausted}
	for attempt, wantOut := range want {
		out, err := s.ApplyFailed(context.Background(), hash, attempt, true, 3, now)
		if err != nil || out != wantOut {
			t.Fatalf("attempt %d: outcome=%v err=%v, want %v", attempt, out, err, wantOut)
		}
	}
	assertDoc(t, s, domain.DocumentFailed, 2, true)
	if total, _ := outboxRows(t, db, hash); total != 3 {
		t.Errorf("outbox rows = %d, want 3", total)
	}
}
