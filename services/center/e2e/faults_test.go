//go:build e2e

package e2e

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// TestPhase2 holds the failure-injection scenarios of `make e2e-faults` (scripts/e2e.sh
// --faults). A scenario that needs an outside action (stop Redis, kill the center) is split
// into an _arrange and a _verify subtest; the script runs arrange, acts, then runs verify. The
// two steps are separate processes, so verify finds arrange's upload by its filename, which
// carries E2E_RUN_ID.
//
// The stub consumer is in mode `indexed` throughout. Nothing here deletes streams: other
// scenarios' entries stay, and every assertion filters by hash.
func TestPhase2(t *testing.T) {
	t.Run("duplicate_upload_concurrent", testDuplicateUploadConcurrent)
	t.Run("redis_outage_short_arrange", testRedisOutageShortArrange)
	t.Run("redis_outage_short_verify", testRedisOutageShortVerify)
	t.Run("redis_outage_long_arrange", testRedisOutageLongArrange)
	t.Run("redis_outage_long_verify", testRedisOutageLongVerify)
	t.Run("hash_kill_mid_claim_arrange", testHashKillArrange)
	t.Run("hash_kill_mid_claim_verify", testHashKillVerify)
	t.Run("outbox_kill_after_publish_arrange", testOutboxKillArrange)
	t.Run("outbox_kill_after_publish_verify", testOutboxKillVerify)
}

// runFile is the upload filename of one scenario in this e2e run.
func runFile(t *testing.T, scenario string) string {
	t.Helper()
	runID := getenv("E2E_RUN_ID", "")
	if runID == "" {
		t.Fatal("E2E_RUN_ID is not set; run the fault scenarios through scripts/e2e.sh --faults")
	}
	return fmt.Sprintf("%s-%s.txt", scenario, runID)
}

// requireYoungClaim fails when a held claim is already half way to stale_threshold: the hung
// process's own sweeper could recover it before the kill lands, which would turn the scenario
// into a different one (extra retries, a third ingest entry).
func requireYoungClaim(t *testing.T, what string, claimedAt sql.NullTime, stale time.Duration) {
	t.Helper()
	if !claimedAt.Valid {
		t.Fatalf("%s has no claimed_at", what)
	}
	if age := time.Since(claimedAt.Time); age >= stale/2 {
		t.Fatalf("%s claim is %v old, not under stale_threshold/2 (%v): the kill would race the hung process's sweeper", what, age, stale/2)
	}
}

// ---- concurrent duplicate upload (edge 5)

// Two uploads of identical bytes confirmed at the same moment. Whether the two hash workers race
// into StartIngestion or one worker takes both in a single pass is up to the scheduler; either
// way there must be one document and one ingestion event, and both uploads must end linked to
// it. The race itself is pinned deterministically by the store test
// TestStartIngestionConcurrentDuplicatesCreateOneDocument.
func testDuplicateUploadConcurrent(t *testing.T) {
	s := newScenario(t)
	content := uniqueContent("duplicate")

	var ids [2]string
	for i := range ids {
		id, uploadURL, err := createIntent(fmt.Sprintf("dup-%d.txt", i))
		if err != nil {
			t.Fatal(err)
		}
		if err := putBytes(uploadURL, content); err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}
	errs := make(chan error, len(ids))
	for _, id := range ids {
		go func() { errs <- confirmUpload(id) }()
	}
	for range ids {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}

	for _, id := range ids {
		if got := waitFinal(t, id, 60*time.Second); got != "INDEXED" {
			t.Fatalf("upload %s ended %s, want INDEXED", id, got)
		}
	}
	h0, h1 := hashOf(t, s.db, ids[0]), hashOf(t, s.db, ids[1])
	s.track(h0)
	if h0 != h1 {
		t.Fatalf("hashes %s and %s differ for identical bytes", h0, h1)
	}
	var docs int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM document_store WHERE hash = ?`, h0).Scan(&docs); err != nil {
		t.Fatal(err)
	}
	if n := outboxCount(t, s.db, h0); docs != 1 || n != 1 {
		t.Errorf("documents = %d, outbox rows = %d; want 1 and 1: only the upload that created the document queues its event", docs, n)
	}
	if got := publishedAttempts(t, s.rdb, h0); len(got) != 1 {
		t.Errorf("ingest events for the hash = %v; want exactly one", got)
	}
	for _, id := range ids {
		if in, err := readIntent(s.db, id); err != nil || in.Status != "HASHED" {
			t.Errorf("intent %s = %+v, %v; want HASHED", id, in, err)
		}
	}
}

// ---- Redis outage during publish

// Runs while Redis is stopped: the upload hashes, and the outbox worker's publish fails and
// releases the row (retries climbs).
func testRedisOutageShortArrange(t *testing.T) {
	s := newDBScenario(t)
	id := upload(t, runFile(t, "outage-short"), uniqueContent("outage-short"))
	hash := s.track(hashOf(t, s.db, id))

	var o outboxState
	if !eventually(30*time.Second, func() bool {
		var err error
		o, err = readOutbox(s.db, hash)
		return err == nil && o.Retries >= 1
	}) {
		t.Fatalf("outbox row = %+v; the failed publish never released the row (want retries >= 1)", o)
	}
	if o.Status == "BLOCKED" {
		t.Fatalf("outbox row already BLOCKED (retries %d); the short outage window is gone, raise [go.outbox] max_retries", o.Retries)
	}
}

// Runs after Redis is healthy again: the row recovers on its own and the document is indexed,
// with exactly one ingest event.
func testRedisOutageShortVerify(t *testing.T) {
	s := newScenario(t)
	id := intentByFilename(t, s.db, runFile(t, "outage-short"))
	hash := s.track(hashOf(t, s.db, id))

	if got := waitFinal(t, id, 60*time.Second); got != "INDEXED" {
		t.Fatalf("upload ended %s, want INDEXED once Redis is back", got)
	}
	o, err := readOutbox(s.db, hash)
	if err != nil {
		t.Fatal(err)
	}
	if max := cfg.Go.Outbox.MaxRetries; o.Status != "PUBLISHED" || o.Retries < 1 || o.Retries >= max {
		t.Errorf("outbox row = %s/retries=%d; want PUBLISHED with 0 < retries < %d (failed attempts counted, never blocked)", o.Status, o.Retries, max)
	}
	if got := publishedAttempts(t, s.rdb, hash); len(got) != 1 {
		t.Errorf("ingest events for the hash = %v; want exactly one (failed XADDs publish nothing)", got)
	}
}

// Runs while Redis is stopped and stays stopped: every publish fails until the row is BLOCKED.
func testRedisOutageLongArrange(t *testing.T) {
	s := newDBScenario(t)
	id := upload(t, runFile(t, "outage-long"), uniqueContent("outage-long"))
	hash := s.track(hashOf(t, s.db, id))

	var o outboxState
	if !eventually(60*time.Second, func() bool {
		var err error
		o, err = readOutbox(s.db, hash)
		return err == nil && o.Status == "BLOCKED"
	}) {
		t.Fatalf("outbox row = %+v; want BLOCKED once retries reach max_retries", o)
	}
	if o.Retries != cfg.Go.Outbox.MaxRetries {
		t.Errorf("BLOCKED with retries = %d; want max_retries %d", o.Retries, cfg.Go.Outbox.MaxRetries)
	}
}

// Runs after Redis is back: a BLOCKED row is the de facto dead letter (B1), so nothing is
// published and the document stays PROCESSING.
func testRedisOutageLongVerify(t *testing.T) {
	s := newScenario(t)
	id := intentByFilename(t, s.db, runFile(t, "outage-long"))
	hash := s.track(hashOf(t, s.db, id))

	time.Sleep(3 * cfg.Go.Outbox.Interval.Std()) // give a wrongly re-queued row time to publish
	o, err := readOutbox(s.db, hash)
	if err != nil {
		t.Fatal(err)
	}
	if o.Status != "BLOCKED" || o.Retries != cfg.Go.Outbox.MaxRetries {
		t.Errorf("outbox row = %s/retries=%d; want it still BLOCKED at %d: nothing re-queues a dead letter (B1)", o.Status, o.Retries, cfg.Go.Outbox.MaxRetries)
	}
	if d := mustDoc(t, s.db, hash); d.Status != "PROCESSING" {
		t.Errorf("document = %s; want PROCESSING (stuck until the B2 reconciliation exists)", d.Status)
	}
	if got := publishedAttempts(t, s.rdb, hash); len(got) != 0 {
		t.Errorf("ingest events for the hash = %v; want none", got)
	}
}

// ---- hash worker killed while it holds a claim

// Runs against a center with CENTER_FAULT=hash-hang: the hash worker claims the intent and
// holds it. The script then SIGKILLs the center.
func testHashKillArrange(t *testing.T) {
	s := newDBScenario(t)
	id := upload(t, runFile(t, "hash-kill"), uniqueContent("hash-kill"))

	var in intentState
	if !eventually(30*time.Second, func() bool {
		var err error
		in, err = readIntent(s.db, id)
		return err == nil && in.Status == "HASHING"
	}) {
		t.Fatalf("intent = %+v; the hooked hash worker never claimed it (is CENTER_FAULT=hash-hang set?)", in)
	}
	requireYoungClaim(t, "intent", in.ClaimedAt, cfg.Go.Hashing.StaleThreshold.Std())
}

// Runs against a fresh center without the fault: the recovery sweep finds the dead claim,
// counts it as a failed attempt, and the intent is hashed and indexed.
func testHashKillVerify(t *testing.T) {
	s := newScenario(t)
	id := intentByFilename(t, s.db, runFile(t, "hash-kill"))

	if got := waitFinal(t, id, 60*time.Second); got != "INDEXED" {
		t.Fatalf("upload ended %s, want INDEXED after the recovery sweep re-queued the dead claim", got)
	}
	hash := s.track(hashOf(t, s.db, id))
	in, err := readIntent(s.db, id)
	if err != nil {
		t.Fatal(err)
	}
	if in.Status != "HASHED" || in.Retries != 1 {
		t.Errorf("intent = %s/retries=%d; want HASHED with retries 1 (the killed attempt counted once)", in.Status, in.Retries)
	}
	if n := outboxCount(t, s.db, hash); n != 1 {
		t.Errorf("outbox rows = %d; want 1", n)
	}
	if got := publishedAttempts(t, s.rdb, hash); len(got) != 1 {
		t.Errorf("ingest events for the hash = %v; want exactly one", got)
	}
}

// ---- outbox worker killed between publish and MarkPublished

// Runs against a center with CENTER_FAULT=outbox-hang-after-publish: the event goes out, the
// consumer indexes the document, and the row stays CLAIMED. The script then SIGKILLs the center.
func testOutboxKillArrange(t *testing.T) {
	s := newScenario(t)
	id := upload(t, runFile(t, "outbox-kill"), uniqueContent("outbox-kill"))
	hash := s.track(hashOf(t, s.db, id))

	if got := waitFinal(t, id, 60*time.Second); got != "INDEXED" {
		t.Fatalf("upload ended %s, want INDEXED from the one published event", got)
	}
	o, err := readOutbox(s.db, hash)
	if err != nil {
		t.Fatal(err)
	}
	if o.Status != "CLAIMED" || o.Retries != 0 {
		t.Fatalf("outbox row = %s/retries=%d; want CLAIMED/0 held by the hook (is CENTER_FAULT=outbox-hang-after-publish set?)", o.Status, o.Retries)
	}
	if got := publishedAttempts(t, s.rdb, hash); len(got) != 1 {
		t.Fatalf("ingest events for the hash = %v; want exactly one before the kill", got)
	}
	requireYoungClaim(t, "outbox row", o.ClaimedAt, cfg.Go.Outbox.StaleThreshold.Std())
}

// Runs against a fresh center without the fault: recovery re-queues the dead claim and the
// event is published a second time. The consumer re-emits INDEXED for the duplicate and Go's
// CAS absorbs it, so the document is untouched.
func testOutboxKillVerify(t *testing.T) {
	s := newScenario(t)
	id := intentByFilename(t, s.db, runFile(t, "outbox-kill"))
	hash := s.track(hashOf(t, s.db, id))

	var o outboxState
	if !eventually(30*time.Second, func() bool {
		var err error
		o, err = readOutbox(s.db, hash)
		return err == nil && o.Status == "PUBLISHED"
	}) {
		t.Fatalf("outbox row = %+v; recovery never re-published the dead claim", o)
	}
	if o.Retries != 1 {
		t.Errorf("outbox retries = %d; want 1 (the killed attempt counted once)", o.Retries)
	}
	if got := publishedAttempts(t, s.rdb, hash); len(got) != 2 {
		t.Errorf("ingest events for the hash = %v; want two: the original and the re-publish", got)
	}
	settleStatusGroup(t, s.rdb)
	if d := mustDoc(t, s.db, hash); d.Status != "INDEXED" || d.Retries != 0 {
		t.Errorf("document = %s/retries=%d; want INDEXED/0: the duplicate INDEXED must be a no-op", d.Status, d.Retries)
	}
}
