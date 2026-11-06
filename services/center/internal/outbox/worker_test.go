package outbox

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"center-service/internal/domain"
)

const docHash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"

var (
	discard  = slog.New(slog.NewTextHandler(io.Discard, nil))
	fixedNow = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
)

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.JobTimeout = time.Second
	return cfg
}

func newTestWorker(store *fakeStore, producer *fakeProducer, cfg Config, logger *slog.Logger) *Worker {
	w := NewWorker("w1", cfg, store, producer, logger)
	w.now = func() time.Time { return fixedNow }
	return w
}

func assertRow(t *testing.T, store *fakeStore, id int64, status domain.OutboxStatus, retries int, why string) {
	t.Helper()
	r := store.row(id)
	if r.status != status || r.retries != retries {
		t.Errorf("outbox row %d = %s/retries=%d; want %s/retries=%d: %s", id, r.status, r.retries, status, retries, why)
	}
	if status != domain.OutboxClaimed && r.claimedBy != "" {
		t.Errorf("outbox row %d still claimed by %q in %s; only a CLAIMED row may name an owner", id, r.claimedBy, r.status)
	}
}

// ---- processMessage outcomes

func TestPassPublishesTheEventAndMarksTheRowPublished(t *testing.T) {
	store, producer := newFakeStore(), &fakeProducer{}
	store.addPending(1, docHash, 2)

	newTestWorker(store, producer, testConfig(), discard).pass(context.Background())

	assertRow(t, store, 1, domain.OutboxPublished, 0, "a published event marks its row PUBLISHED")
	got := producer.events()
	if len(got) != 1 {
		t.Fatalf("published %d events; want exactly 1", len(got))
	}
	e := got[0]
	if e.Hash != docHash || e.Filename != "a.pdf" || e.BucketPath != "ba/78/"+docHash || e.Size != 42 ||
		e.Attempts != 2 || !e.CreatedAt.Equal(fixedNow) || e.SchemaVersion != testConfig().SchemaVersion || e.ID == "" {
		t.Errorf("event = %+v; want the document's hash, filename, bucket key, size, attempts = document retries (2), "+
			"created_at from the worker clock, the configured schema_version and a trace id", e)
	}
}

func TestEveryPublishGetsANewTraceID(t *testing.T) {
	store, producer := newFakeStore(), &fakeProducer{}
	store.addPending(1, docHash, 0)
	store.addPending(2, "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed", 0)

	newTestWorker(store, producer, testConfig(), discard).pass(context.Background())

	got := producer.events()
	if len(got) != 2 || got[0].ID == got[1].ID {
		t.Errorf("trace ids = %v; want two different ids: the id is per publish, for tracing only", got)
	}
}

func TestMissingDocumentBlocksTheRowWithoutPublishing(t *testing.T) {
	store, producer := newFakeStore(), &fakeProducer{}
	store.addPending(1, docHash, 0)
	delete(store.documents, docHash)

	newTestWorker(store, producer, testConfig(), discard).pass(context.Background())

	assertRow(t, store, 1, domain.OutboxBlocked, 0, "an event for a missing document can never succeed: BLOCKED at once, no retry")
	if n := len(producer.events()); n != 0 {
		t.Errorf("published %d events; want none for a missing document", n)
	}
}

func TestTransientFailureReleasesTheRowForARetry(t *testing.T) {
	cases := map[string]func(*fakeStore, *fakeProducer){
		"document read fails": func(s *fakeStore, _ *fakeProducer) { s.errs["GetDocumentByHash"] = errors.New("db busy") },
		"publish fails":       func(_ *fakeStore, p *fakeProducer) { p.err = errors.New("redis down") },
	}
	for name, inject := range cases {
		t.Run(name, func(t *testing.T) {
			store, producer := newFakeStore(), &fakeProducer{}
			store.addPending(1, docHash, 0)
			inject(store, producer)

			newTestWorker(store, producer, testConfig(), discard).pass(context.Background())

			assertRow(t, store, 1, domain.OutboxPending, 1, "a transient failure releases the row and counts one retry; it must not be marked PUBLISHED")
		})
	}
}

func TestTransientFailureAtTheLastRetryBlocksTheRow(t *testing.T) {
	store, producer := newFakeStore(), &fakeProducer{err: errors.New("redis down")}
	store.addPending(1, docHash, 0)
	store.rows[1].retries = testConfig().MaxRetries - 1

	newTestWorker(store, producer, testConfig(), discard).pass(context.Background())

	assertRow(t, store, 1, domain.OutboxBlocked, testConfig().MaxRetries, "the release that reaches MaxRetries ends the row BLOCKED")
}

// A publish that outlives JobTimeout must still release the row: the release uses the
// worker's context, not the expired job context.
func TestJobTimeoutStillReleasesTheRow(t *testing.T) {
	store, producer := newFakeStore(), &fakeProducer{blockPublish: true}
	store.addPending(1, docHash, 0)
	cfg := testConfig()
	cfg.JobTimeout = 20 * time.Millisecond

	newTestWorker(store, producer, cfg, discard).pass(context.Background())

	assertRow(t, store, 1, domain.OutboxPending, 1, "a timed-out publish must release with the worker context, not the expired job context")
}

// The event is out: if recording that fails, the row must stay CLAIMED for the recovery sweep.
// Releasing it would re-queue at once and count a retry for an event that was delivered.
func TestMarkPublishedFailureLeavesTheClaimForRecovery(t *testing.T) {
	store, producer := newFakeStore(), &fakeProducer{}
	store.addPending(1, docHash, 0)
	store.errs["MarkPublished"] = errors.New("db busy")

	newTestWorker(store, producer, testConfig(), discard).pass(context.Background())

	if r := store.row(1); r.status != domain.OutboxClaimed || r.claimedBy != "w1" || r.retries != 0 {
		t.Errorf("row = %s/%s/retries=%d; want CLAIMED by w1, retries 0: a delivered event must not be released", r.status, r.claimedBy, r.retries)
	}
	if n := len(producer.events()); n != 1 {
		t.Errorf("published %d events; want 1", n)
	}
}

// The recovery sweep re-queued the row and another worker claimed it while this worker was
// publishing. This worker must leave the new owner's row alone.
func TestLostClaimAfterPublishLeavesTheNewOwnersRowAlone(t *testing.T) {
	store := newFakeStore()
	producer := &fakeProducer{onPublish: func() {
		store.mu.Lock()
		store.rows[1].claimedBy, store.rows[1].retries = "w2", 1
		store.mu.Unlock()
	}}
	store.addPending(1, docHash, 0)
	var buf bytes.Buffer

	newTestWorker(store, producer, testConfig(), slog.New(slog.NewTextHandler(&buf, nil))).pass(context.Background())

	if r := store.row(1); r.status != domain.OutboxClaimed || r.claimedBy != "w2" || r.retries != 1 {
		t.Errorf("row = %s/%s/retries=%d; want CLAIMED by w2 with retries 1 untouched", r.status, r.claimedBy, r.retries)
	}
	if !strings.Contains(buf.String(), "lost claim") {
		t.Errorf("log = %q; want a lost-claim warning so a duplicate event can be explained", buf.String())
	}
}

// ---- pass control flow

func TestPassStopsWhenFetchingFails(t *testing.T) {
	store, producer := newFakeStore(), &fakeProducer{}
	store.addPending(1, docHash, 0)
	store.errs["FetchCandidates"] = errors.New("db locked")

	newTestWorker(store, producer, testConfig(), discard).pass(context.Background())

	assertRow(t, store, 1, domain.OutboxPending, 0, "nothing is claimed when the fetch fails")
}

func TestPassSkipsRowsItCouldNotClaimAndContinues(t *testing.T) {
	store, producer := newFakeStore(), &fakeProducer{}
	store.addPending(1, docHash, 0)
	store.addPending(2, "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed", 0)
	store.onClaim = func(id int64) {
		if id == 1 { // another worker wins row 1 between our fetch and our claim
			store.mu.Lock()
			store.rows[1].status, store.rows[1].claimedBy = domain.OutboxClaimed, "w2"
			store.mu.Unlock()
		}
	}

	newTestWorker(store, producer, testConfig(), discard).pass(context.Background())

	if r := store.row(1); r.claimedBy != "w2" {
		t.Errorf("row 1 owned by %q; want w2 (the claim loser must skip it)", r.claimedBy)
	}
	assertRow(t, store, 2, domain.OutboxPublished, 0, "a lost race on one row must not stop the rest of the batch")
	if n := len(producer.events()); n != 1 {
		t.Errorf("published %d events; want 1 (only the row this worker won)", n)
	}
}

func TestPassContinuesAfterAClaimError(t *testing.T) {
	store, producer := newFakeStore(), &fakeProducer{}
	store.addPending(1, docHash, 0)
	store.addPending(2, "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed", 0)
	store.onClaim = func(id int64) {
		store.mu.Lock()
		defer store.mu.Unlock()
		if id == 1 {
			store.errs["ClaimOutboxMessage"] = errors.New("db busy")
		} else {
			delete(store.errs, "ClaimOutboxMessage")
		}
	}

	newTestWorker(store, producer, testConfig(), discard).pass(context.Background())

	assertRow(t, store, 1, domain.OutboxPending, 0, "a claim error skips the row without counting a retry")
	assertRow(t, store, 2, domain.OutboxPublished, 0, "one claim error must not stop the batch")
}

func TestPassStopsBetweenRowsWhenCancelled(t *testing.T) {
	store := newFakeStore()
	ctx, cancel := context.WithCancel(context.Background())
	producer := &fakeProducer{onPublish: cancel} // shutdown arrives after the first publish
	store.addPending(1, docHash, 0)
	store.addPending(2, "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed", 0)

	newTestWorker(store, producer, testConfig(), discard).pass(ctx)

	// The claim fake honours ctx, so row 2 would stay PENDING even if pass tried to claim it;
	// only the attempt count shows the loop stopped.
	if store.claims != 1 {
		t.Errorf("claim attempts = %d; want 1: pass must check ctx before claiming the next row", store.claims)
	}
	assertRow(t, store, 2, domain.OutboxPending, 0, "a cancelled pass must not claim further rows")
}

func TestRunStopsOnCancel(t *testing.T) {
	cfg := testConfig()
	cfg.Interval = time.Millisecond
	w := newTestWorker(newFakeStore(), &fakeProducer{}, cfg, discard)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
