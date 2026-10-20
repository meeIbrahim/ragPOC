package status

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"center-service/internal/domain"
	"center-service/internal/events"
)

var fixedNow = time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)

type applyIndexedCall struct {
	hash     string
	attempts int
	now      time.Time
}

type applyFailedCall struct {
	hash        string
	attempts    int
	retryable   bool
	maxAttempts int
	now         time.Time
}

type fakeStore struct {
	indexed []applyIndexedCall
	failed  []applyFailedCall

	indexedResult bool
	failedResult  domain.FailedOutcome
	err           error

	deadline time.Time
	hadDL    bool
}

func (f *fakeStore) ApplyIndexed(ctx context.Context, hash string, attempts int, now time.Time) (bool, error) {
	f.deadline, f.hadDL = ctx.Deadline()
	f.indexed = append(f.indexed, applyIndexedCall{hash, attempts, now})
	return f.indexedResult, f.err
}

func (f *fakeStore) ApplyFailed(ctx context.Context, hash string, attempts int, retryable bool, maxAttempts int, now time.Time) (domain.FailedOutcome, error) {
	f.deadline, f.hadDL = ctx.Deadline()
	f.failed = append(f.failed, applyFailedCall{hash, attempts, retryable, maxAttempts, now})
	return f.failedResult, f.err
}

func (f *fakeStore) calls() int { return len(f.indexed) + len(f.failed) }

func newHandler(store Store) *Handler {
	h := NewHandler(store, 3, 5*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h.now = func() time.Time { return fixedNow }
	return h
}

func fixtureDir() string {
	return filepath.Join("..", "..", "..", "..", "contracts", "events", "fixtures")
}

func entryFromFixture(t *testing.T, rel string) events.StreamEntry {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixtureDir(), rel))
	if err != nil {
		t.Fatal(err)
	}
	return events.StreamEntry{ID: "1-0", Values: map[string]any{
		"event_type": events.StatusEventType,
		"payload":    string(raw),
	}}
}

const fixtureHash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"

func TestHandleIndexedAppliesAndAcks(t *testing.T) {
	for _, applied := range []bool{true, false} { // a duplicate is still acked
		store := &fakeStore{indexedResult: applied}
		if ack := newHandler(store).Handle(context.Background(), entryFromFixture(t, "status_indexed.json")); !ack {
			t.Fatalf("applied=%v: entry not acked", applied)
		}
		want := applyIndexedCall{fixtureHash, 0, fixedNow}
		if len(store.indexed) != 1 || store.indexed[0] != want || len(store.failed) != 0 {
			t.Fatalf("applied=%v: calls = %+v / %+v, want one %+v", applied, store.indexed, store.failed, want)
		}
	}
}

func TestHandleFailedPassesRetryableAndMaxAttempts(t *testing.T) {
	tests := []struct {
		fixture string
		want    applyFailedCall
	}{
		{"status_failed_retryable.json", applyFailedCall{fixtureHash, 1, true, 3, fixedNow}},
		{"status_failed_permanent.json", applyFailedCall{fixtureHash, 0, false, 3, fixedNow}},
		{"status_failed_poison.json", applyFailedCall{fixtureHash, 2, false, 3, fixedNow}},
	}
	for _, tc := range tests {
		for _, outcome := range []domain.FailedOutcome{domain.FailedStale, domain.FailedRetried, domain.FailedExhausted} {
			store := &fakeStore{failedResult: outcome}
			if ack := newHandler(store).Handle(context.Background(), entryFromFixture(t, tc.fixture)); !ack {
				t.Fatalf("%s/%v: entry not acked", tc.fixture, outcome)
			}
			if len(store.failed) != 1 || store.failed[0] != tc.want || len(store.indexed) != 0 {
				t.Fatalf("%s/%v: calls = %+v / %+v, want one %+v", tc.fixture, outcome, store.failed, store.indexed, tc.want)
			}
		}
	}
}

// Every invalid contract fixture is discarded and acked without touching the store: retrying
// the same bytes can never succeed, and an unacked entry would be redelivered forever.
func TestHandleDiscardsInvalidFixtures(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(fixtureDir(), "invalid", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no invalid fixtures (err %v)", err)
	}
	for _, f := range files {
		store := &fakeStore{}
		rel := filepath.Join("invalid", filepath.Base(f))
		if ack := newHandler(store).Handle(context.Background(), entryFromFixture(t, rel)); !ack {
			t.Errorf("%s: not acked", rel)
		}
		if store.calls() != 0 {
			t.Errorf("%s: store was called", rel)
		}
	}
}

func TestHandleDiscardsMalformedEntries(t *testing.T) {
	valid := entryFromFixture(t, "status_indexed.json").Values["payload"]
	tests := map[string]map[string]any{
		"no fields":           {},
		"missing event_type":  {"payload": valid},
		"wrong event_type":    {"event_type": "IngestionRequest", "payload": valid},
		"missing payload":     {"event_type": events.StatusEventType},
		"payload not text":    {"event_type": events.StatusEventType, "payload": 42},
		"empty payload":       {"event_type": events.StatusEventType, "payload": ""},
		"event_type not text": {"event_type": 7, "payload": valid},
	}
	for name, values := range tests {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{}
			if ack := newHandler(store).Handle(context.Background(), events.StreamEntry{ID: "1-0", Values: values}); !ack {
				t.Error("not acked")
			}
			if store.calls() != 0 {
				t.Error("store was called")
			}
		})
	}
}

func TestHandleAcceptsBytePayload(t *testing.T) {
	e := entryFromFixture(t, "status_indexed.json")
	e.Values["payload"] = []byte(e.Values["payload"].(string))
	store := &fakeStore{indexedResult: true}
	if ack := newHandler(store).Handle(context.Background(), e); !ack || len(store.indexed) != 1 {
		t.Fatalf("ack=%v calls=%d", ack, len(store.indexed))
	}
}

// A database error must leave the entry pending so it is retried; acking would drop the event.
func TestHandleStoreErrorDoesNotAck(t *testing.T) {
	for _, fixture := range []string{"status_indexed.json", "status_failed_retryable.json"} {
		store := &fakeStore{err: errors.New("database is locked")}
		if ack := newHandler(store).Handle(context.Background(), entryFromFixture(t, fixture)); ack {
			t.Errorf("%s: acked despite a store error", fixture)
		}
	}
}

func TestHandleBoundsTheStoreCallWithEntryTimeout(t *testing.T) {
	store := &fakeStore{indexedResult: true}
	before := time.Now()
	newHandler(store).Handle(context.Background(), entryFromFixture(t, "status_indexed.json"))
	if !store.hadDL {
		t.Fatal("store context has no deadline")
	}
	if got := store.deadline.Sub(before); got > 5*time.Second+time.Second || got < 4*time.Second {
		t.Fatalf("deadline is %v after the call started, want about 5s", got)
	}
}

func TestHandleDoesNotAckWhenParentContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := &fakeStore{err: ctx.Err()}
	if ack := newHandler(store).Handle(ctx, entryFromFixture(t, "status_indexed.json")); ack {
		t.Fatal("acked during shutdown; the entry must stay pending")
	}
}

// INDEXED ignores retryable/reason/chunk_count (PRD 4.3 only uses attempts).
func TestHandleIndexedIgnoresRetryableAndReason(t *testing.T) {
	e := events.StreamEntry{ID: "1-0", Values: map[string]any{
		"event_type": events.StatusEventType,
		"payload": `{"id":"x","hash":"` + fixtureHash + `","status":"INDEXED","attempts":2,` +
			`"retryable":true,"reason":"INTERNAL","created_at":"2026-10-14T10:00:00Z","schema_version":1}`,
	}}
	store := &fakeStore{indexedResult: true}
	if ack := newHandler(store).Handle(context.Background(), e); !ack {
		t.Fatal("not acked")
	}
	if len(store.failed) != 0 || len(store.indexed) != 1 || store.indexed[0].attempts != 2 {
		t.Fatalf("calls = %+v / %+v", store.indexed, store.failed)
	}
}
