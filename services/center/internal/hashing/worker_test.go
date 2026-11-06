package hashing

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

const abcHash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.JobTimeout = time.Second
	return cfg
}

func newTestWorker(store *fakeStore, blob *fakeBlob) *Worker {
	return NewWorker("w1", testConfig(), store, blob, discard)
}

func strp(s string) *string { return &s }

// uploaded seeds one UPLOADED intent whose temp object holds "abc".
func uploaded(store *fakeStore, blob *fakeBlob, id string) {
	store.addUploaded(id, strp(id))
	blob.put(id, []byte("abc"))
}

func assertIntent(t *testing.T, store *fakeStore, id string, status domain.IntentState, retries int, why string) {
	t.Helper()
	in := store.intent(id)
	if in.status != status || in.retries != retries {
		t.Errorf("intent %s = %s/retries=%d; want %s/retries=%d: %s", id, in.status, in.retries, status, retries, why)
	}
	// A release or a failure hands the intent back, so it must drop the claim. (HASHED keeps
	// claimed_by, as the SQL does: nothing reads a claim outside HASHING.)
	if (status == domain.IntentUploaded || status == domain.IntentFailed) && in.claimedBy != "" {
		t.Errorf("intent %s still claimed by %q in %s; a release or failure must clear its claim", id, in.claimedBy, in.status)
	}
}

// ---- process outcomes

func TestPassHashesMovesAndRecordsAnUploadedIntent(t *testing.T) {
	store, blob := newFakeStore(), newFakeBlob()
	uploaded(store, blob, "i1")

	newTestWorker(store, blob).pass(context.Background())

	assertIntent(t, store, "i1", domain.IntentHashed, 0, "a readable upload ends HASHED")
	if in := store.intent("i1"); in.hash != abcHash {
		t.Errorf("intent linked to %q; want the content hash %s", in.hash, abcHash)
	}
	d := store.documents[abcHash]
	if d == nil || d.Size != 3 || d.FileName != "i1.txt" || d.PermBucketPath != "ba/78/"+abcHash {
		t.Errorf("document = %+v; want hash %s, size 3, the intent's filename and the ingestion key", d, abcHash)
	}
	if _, inTemp := blob.uploads["i1"]; inTemp || blob.ingestion[abcHash] == nil {
		t.Errorf("object still in the upload bucket or missing from ingestion; the move must remove the temp object")
	}
}

func TestMissingTempObjectFailsTheIntentWithoutARetry(t *testing.T) {
	cases := map[string]func(*fakeStore, *fakeBlob){
		"no temp path":   func(s *fakeStore, _ *fakeBlob) { s.addUploaded("i1", nil) },
		"object deleted": func(s *fakeStore, _ *fakeBlob) { s.addUploaded("i1", strp("i1")) },
	}
	for name, seed := range cases {
		t.Run(name, func(t *testing.T) {
			store, blob := newFakeStore(), newFakeBlob()
			seed(store, blob)

			newTestWorker(store, blob).pass(context.Background())

			assertIntent(t, store, "i1", domain.IntentFailed, 0, "a missing upload is permanent: FAILED at once, no retry consumed")
			if len(store.documents) != 0 {
				t.Errorf("documents = %d; want none for a failed intent", len(store.documents))
			}
		})
	}
}

func TestTransientFailureReleasesTheClaimForARetry(t *testing.T) {
	for _, method := range []string{"ExistsInUploadBucket", "HashUploadObject", "MigrateToIngestion", "StartIngestion"} {
		t.Run(method, func(t *testing.T) {
			store, blob := newFakeStore(), newFakeBlob()
			uploaded(store, blob, "i1")
			blob.errs[method] = errors.New("minio down")
			store.errs[method] = errors.New("db busy")

			newTestWorker(store, blob).pass(context.Background())

			assertIntent(t, store, "i1", domain.IntentUploaded, 1, "a transient error releases the claim and counts one retry")
			if len(store.documents) != 0 || store.events != 0 {
				t.Errorf("documents = %d, events = %d; want none after a failed attempt", len(store.documents), store.events)
			}
		})
	}
}

func TestTransientFailureAtTheLastRetryFailsTheIntent(t *testing.T) {
	store, blob := newFakeStore(), newFakeBlob()
	uploaded(store, blob, "i1")
	store.intents["i1"].retries = testConfig().MaxRetries - 1
	blob.errs["HashUploadObject"] = errors.New("minio down")

	newTestWorker(store, blob).pass(context.Background())

	assertIntent(t, store, "i1", domain.IntentFailed, testConfig().MaxRetries, "the release that reaches MaxRetries ends the intent FAILED")
}

// The recovery sweep handed the intent to another worker while this one was hashing. This
// worker must leave the new owner's claim alone: no release, no failure.
func TestLostClaimLeavesTheNewOwnersIntentAlone(t *testing.T) {
	store, blob := newFakeStore(), newFakeBlob()
	uploaded(store, blob, "i1")
	store.onStartIngestion = func(id string) { store.steal(id, "w2") }

	newTestWorker(store, blob).pass(context.Background())

	in := store.intent("i1")
	if in.status != domain.IntentHashing || in.claimedBy != "w2" || in.retries != 0 {
		t.Errorf("intent = %s/%s/retries=%d; want HASHING owned by w2 with retries 0: a lost claim must not be released or failed",
			in.status, in.claimedBy, in.retries)
	}
}

// A job that outlives JobTimeout must still release its claim: the release uses the worker's
// context, not the expired job context.
func TestJobTimeoutStillReleasesTheClaim(t *testing.T) {
	store, blob := newFakeStore(), newFakeBlob()
	uploaded(store, blob, "i1")
	blob.blockHash = true
	cfg := testConfig()
	cfg.JobTimeout = 20 * time.Millisecond

	NewWorker("w1", cfg, store, blob, discard).pass(context.Background())

	assertIntent(t, store, "i1", domain.IntentUploaded, 1, "a timed-out job must release with the worker context, not the expired job context")
}

// ---- pass control flow

func TestPassStopsWhenFetchingFails(t *testing.T) {
	store, blob := newFakeStore(), newFakeBlob()
	uploaded(store, blob, "i1")
	store.errs["GetIntentsForHashing"] = errors.New("db locked")

	newTestWorker(store, blob).pass(context.Background())

	assertIntent(t, store, "i1", domain.IntentUploaded, 0, "nothing is claimed when the fetch fails")
}

func TestPassSkipsAnIntentItCouldNotClaimAndContinues(t *testing.T) {
	store, blob := newFakeStore(), newFakeBlob()
	uploaded(store, blob, "lost")
	uploaded(store, blob, "errored")
	store.addUploaded("ok", strp("ok"))
	blob.put("ok", []byte("ok"))
	// another worker wins "lost" between our fetch and our claim
	store.onClaim = func(id string) {
		if id == "lost" {
			store.mu.Lock()
			store.intents[id].status, store.intents[id].claimedBy = domain.IntentHashing, "w2"
			store.mu.Unlock()
		}
	}
	store.errs["UpdateIntentToHashing:errored"] = errors.New("db busy")

	newTestWorker(store, blob).pass(context.Background())

	if in := store.intent("lost"); in.claimedBy != "w2" {
		t.Errorf("lost intent owned by %q; want w2 (the claim loser must skip it)", in.claimedBy)
	}
	assertIntent(t, store, "errored", domain.IntentUploaded, 0, "a claim error skips the intent without counting a retry")
	assertIntent(t, store, "ok", domain.IntentHashed, 0, "one bad intent must not stop the rest of the batch")
}

func TestPassStopsBetweenIntentsWhenCancelled(t *testing.T) {
	store, blob := newFakeStore(), newFakeBlob()
	uploaded(store, blob, "first")
	store.addUploaded("second", strp("second"))
	blob.put("second", []byte("second"))
	ctx, cancel := context.WithCancel(context.Background())
	blob.onHash = func(string) { cancel() } // shutdown arrives while the first intent is processed

	newTestWorker(store, blob).pass(ctx)

	// The claim fake honours ctx, so "second" would stay UPLOADED even if pass tried to claim
	// it; only the attempt count shows the loop stopped.
	if store.claims != 1 {
		t.Errorf("claim attempts = %d; want 1: pass must check ctx before claiming the next intent", store.claims)
	}
	assertIntent(t, store, "second", domain.IntentUploaded, 0, "a cancelled pass must not claim further intents")
}

func TestRunStopsOnCancel(t *testing.T) {
	cfg := testConfig()
	cfg.Interval = time.Millisecond
	w := NewWorker("w1", cfg, newFakeStore(), newFakeBlob(), discard)
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

// ---- logging

func TestFailuresAreLoggedWithTheIntent(t *testing.T) {
	store, blob := newFakeStore(), newFakeBlob()
	uploaded(store, blob, "i1")
	blob.errs["HashUploadObject"] = errors.New("minio down")
	var buf bytes.Buffer
	w := NewWorker("w1", testConfig(), store, blob, slog.New(slog.NewTextHandler(&buf, nil)))

	w.pass(context.Background())

	if out := buf.String(); !strings.Contains(out, "intent=i1") || !strings.Contains(out, "minio down") {
		t.Errorf("log = %q; want the intent id and the cause, so a stuck upload can be traced", out)
	}
}
