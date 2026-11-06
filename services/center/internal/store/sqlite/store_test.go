package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

	"center-service/internal/domain"
	"center-service/internal/store/sqlite"
)

func newDocument(h string) *domain.Document {
	return &domain.Document{Hash: h, PermBucketPath: h[:2] + "/" + h[2:4] + "/" + h, FileName: "a.txt", Size: 3}
}

// newClockedStore is newStore with the store clock pinned at now.
func newClockedStore(t *testing.T) (*sqlite.Store, *sql.DB) {
	t.Helper()
	s, db := newStore(t)
	sqlite.SetNow(s, fixedClock(now))
	return s, db
}

func TestStartIngestionOfANewHashCreatesDocumentAndOutboxRow(t *testing.T) {
	s, db := newClockedStore(t)
	seedIntent(t, db, "i1", intentSeed{status: domain.IntentHashing, claimedBy: "w1", claimedAt: now})

	if err := s.StartIngestion(context.Background(), newDocument(hash), "i1", "w1"); err != nil {
		t.Fatalf("start ingestion: %v", err)
	}

	d := getDoc(t, s, hash)
	if d.Status != domain.DocumentProcessing || d.Retries != 0 || !d.CreatedAt.Equal(now) || !d.UpdatedAt.Equal(now) {
		t.Errorf("document = %s/retries=%d/created=%v/updated=%v; want PROCESSING/0 stamped at %v", d.Status, d.Retries, d.CreatedAt, d.UpdatedAt, now)
	}
	if total, pending := outboxRows(t, db, hash); total != 1 || pending != 1 {
		t.Errorf("outbox rows = %d (pending %d); want exactly 1 PENDING: a new document queues one ingestion event", total, pending)
	}
	if r := readIntent(t, db, "i1"); r.status != "HASHED" || r.hash.String != hash {
		t.Errorf("intent = %s/hash=%q; want HASHED and linked to %s", r.status, r.hash.String, hash)
	}
}

func TestStartIngestionOfAKnownHashOnlyLinksTheIntent(t *testing.T) {
	s, db := newClockedStore(t)
	insertDoc(t, db, hash, domain.DocumentIndexed, 1)
	seedIntent(t, db, "i2", intentSeed{status: domain.IntentHashing, claimedBy: "w1", claimedAt: now})

	if err := s.StartIngestion(context.Background(), newDocument(hash), "i2", "w1"); err != nil {
		t.Fatalf("start ingestion: %v", err)
	}

	d := getDoc(t, s, hash)
	if d.Status != domain.DocumentIndexed || d.Retries != 1 || !d.UpdatedAt.Equal(created) {
		t.Errorf("document = %s/retries=%d/updated=%v; want the existing INDEXED/1 row untouched", d.Status, d.Retries, d.UpdatedAt)
	}
	if total, _ := outboxRows(t, db, hash); total != 0 {
		t.Errorf("outbox rows = %d; want 0: a duplicate upload must not queue another ingestion event", total)
	}
	if r := readIntent(t, db, "i2"); r.status != "HASHED" || r.hash.String != hash {
		t.Errorf("intent = %s/hash=%q; want HASHED and linked to the existing document", r.status, r.hash.String)
	}
}

func TestStartIngestionAfterALostClaimWritesNothing(t *testing.T) {
	s, db := newClockedStore(t)
	seedIntent(t, db, "i1", intentSeed{status: domain.IntentHashing, claimedBy: "w2", claimedAt: now})

	err := s.StartIngestion(context.Background(), newDocument(hash), "i1", "w1")
	if !errors.Is(err, domain.ErrLostClaim) {
		t.Fatalf("err = %v; want domain.ErrLostClaim when another worker holds the intent", err)
	}

	var docs int
	if err := db.QueryRow(`SELECT COUNT(*) FROM document_store`).Scan(&docs); err != nil {
		t.Fatal(err)
	}
	if total, _ := outboxRows(t, db, hash); docs != 0 || total != 0 {
		t.Errorf("documents = %d, outbox rows = %d; want 0 and 0: a lost claim must roll back the whole transaction", docs, total)
	}
	if r := readIntent(t, db, "i1"); r.status != "HASHING" || r.claimedBy.String != "w2" || r.hash.Valid {
		t.Errorf("intent = %s/%s/hash=%v; want the new owner's HASHING claim untouched", r.status, r.claimedBy.String, r.hash)
	}
}

// Two uploads of identical bytes hashed at the same time: the document and its event must
// exist once, and both jobs must end linked to it.
func TestStartIngestionConcurrentDuplicatesCreateOneDocument(t *testing.T) {
	s, db := newClockedStore(t)
	seedIntent(t, db, "i1", intentSeed{status: domain.IntentHashing, claimedBy: "w1", claimedAt: now})
	seedIntent(t, db, "i2", intentSeed{status: domain.IntentHashing, claimedBy: "w2", claimedAt: now})

	var wg sync.WaitGroup
	for _, c := range []struct{ intent, worker string }{{"i1", "w1"}, {"i2", "w2"}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.StartIngestion(context.Background(), newDocument(hash), c.intent, c.worker); err != nil {
				t.Errorf("start ingestion %s: %v; a duplicate hash must link, not fail", c.intent, err)
			}
		}()
	}
	wg.Wait()

	var docs int
	if err := db.QueryRow(`SELECT COUNT(*) FROM document_store WHERE hash = ?`, hash).Scan(&docs); err != nil {
		t.Fatal(err)
	}
	if total, _ := outboxRows(t, db, hash); docs != 1 || total != 1 {
		t.Errorf("documents = %d, outbox rows = %d; want 1 and 1: only the upload that created the document queues its event", docs, total)
	}
	for _, id := range []string{"i1", "i2"} {
		if r := readIntent(t, db, id); r.status != "HASHED" || r.hash.String != hash {
			t.Errorf("intent %s = %s/hash=%q; want HASHED and linked", id, r.status, r.hash.String)
		}
	}
}

// ---- GetDocumentByHash

func TestGetDocumentByHash(t *testing.T) {
	s, db := newStore(t)
	insertDoc(t, db, hash, domain.DocumentIndexed, 2)

	d, err := s.GetDocumentByHash(context.Background(), hash)
	if err != nil || d == nil {
		t.Fatalf("get = %v, %v; want the document", d, err)
	}
	if d.Hash != hash || d.PermBucketPath != "ba/78/"+hash || d.FileName != "a.txt" || d.Size != 3 ||
		d.Status != domain.DocumentIndexed || d.Retries != 2 || !d.CreatedAt.Equal(created) || !d.UpdatedAt.Equal(created) {
		t.Errorf("document = %+v; want every column read back", d)
	}

	missing, err := s.GetDocumentByHash(context.Background(), "0000")
	if err != nil || missing != nil {
		t.Errorf("get unknown hash = %v, %v; want nil, nil (a missing document is not an error)", missing, err)
	}
}

// Every move out of HASHING clears claimed_by, so this status fence is defence in depth.
// Seed the state it guards against directly: a FAILED intent still naming w1.
func TestStartIngestionNeedsAHashingIntent(t *testing.T) {
	s, db := newClockedStore(t)
	seedIntent(t, db, "i1", intentSeed{status: domain.IntentFailed, claimedBy: "w1", claimedAt: now})

	err := s.StartIngestion(context.Background(), newDocument(hash), "i1", "w1")
	if !errors.Is(err, domain.ErrLostClaim) {
		t.Fatalf("err = %v; want domain.ErrLostClaim: StartIngestion must be fenced on status=HASHING", err)
	}
	var docs int
	if err := db.QueryRow(`SELECT COUNT(*) FROM document_store`).Scan(&docs); err != nil {
		t.Fatal(err)
	}
	if docs != 0 {
		t.Errorf("documents = %d; want 0: the refused intent update must roll back the document insert", docs)
	}
}
