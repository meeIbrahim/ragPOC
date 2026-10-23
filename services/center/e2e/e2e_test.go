//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// TestPhase0 holds one subtest per scenario. scripts/e2e.sh sets the stub consumer's mode and
// then runs a single subtest, e.g. `-test.run 'TestPhase0/crash$'`; running a subtest while the
// stub is in another mode fails, as it should.
func TestPhase0(t *testing.T) {
	t.Run("indexed", testIndexed)
	t.Run("failed_retryable", testFailedRetryable)
	t.Run("failed_permanent", testFailedPermanent)
	t.Run("slow", testSlow)
	t.Run("crash", testCrash)
	t.Run("hang", testHang)
	t.Run("unsupported_ext", testUnsupportedExtension)
	t.Run("consumer_cleanup", testConsumerCleanup)
}

// stub mode: indexed
func testIndexed(t *testing.T) {
	s := newScenario(t)
	resetStreams(t, s.rdb)

	id := upload(t, "indexed.txt", uniqueContent("indexed"))
	if got := waitFinal(t, id, 60*time.Second); got != "INDEXED" {
		t.Fatalf("final status = %s, want INDEXED", got)
	}
	hash := s.track(hashOf(t, s.db, id))

	if d := mustDoc(t, s.db, hash); d != (docRow{"INDEXED", 0}) {
		t.Fatalf("document = %+v, want INDEXED with retries=0", d)
	}
	if n := outboxCount(t, s.db, hash); n != 1 {
		t.Fatalf("outbox rows = %d, want 1", n)
	}
	events := statusEventsFor(t, s.rdb, hash)
	if len(events) != 1 || events[0].Status != "INDEXED" || events[0].Attempts != 0 {
		t.Fatalf("status events = %+v, want one INDEXED with attempts=0", events)
	}

	// Redelivering any status event is a no-op: not even the row's updated_at moves.
	stamp := updatedAt(t, s.db, hash)
	for _, ev := range events {
		addStatusEntry(t, s.rdb, ev.Payload)
	}
	settleStatusGroup(t, s.rdb)
	assertUnchanged(t, s, hash, docRow{"INDEXED", 0}, 1)
	assertStamp(t, s, hash, stamp)

	// A FAILED(N) arriving after INDEXED(N) leaves the document INDEXED.
	addStatusEntry(t, s.rdb, failedPayload(hash, 0, true, "INTERNAL"))
	settleStatusGroup(t, s.rdb)
	assertUnchanged(t, s, hash, docRow{"INDEXED", 0}, 1)
	assertStamp(t, s, hash, stamp)
}

// stub mode: failed-retryable
func testFailedRetryable(t *testing.T) {
	s := newScenario(t)
	resetStreams(t, s.rdb)

	id := upload(t, "retry.txt", uniqueContent("failed-retryable"))
	if got := waitFinal(t, id, 90*time.Second); got != "FAILED" {
		t.Fatalf("final status = %s, want FAILED", got)
	}
	hash := s.track(hashOf(t, s.db, id))

	// max_attempts=3: attempts 0, 1, 2 fail; the third failure is final.
	assertUnchanged(t, s, hash, docRow{"FAILED", maxAttempts - 1}, maxAttempts)
	events := statusEventsFor(t, s.rdb, hash)
	if len(events) != maxAttempts {
		t.Fatalf("status events = %d, want %d: %+v", len(events), maxAttempts, events)
	}
	for i, ev := range events {
		if ev.Status != "FAILED" || ev.Attempts != i || ev.Reason != "INTERNAL" || ev.Retryable == nil || !*ev.Retryable {
			t.Errorf("event %d = %+v, want retryable FAILED INTERNAL with attempts=%d", i, ev, i)
		}
	}
	// The center published one ingest event per attempt, with the attempt number it holds.
	if got := publishedAttempts(t, s.rdb, hash); len(got) != maxAttempts || got[0] != 0 || got[1] != 1 || got[2] != 2 {
		t.Errorf("published ingest attempts = %v, want [0 1 2]", got)
	}

	// Redelivering the three FAILED events must not queue another attempt or touch the row.
	stamp := updatedAt(t, s.db, hash)
	for _, ev := range events {
		addStatusEntry(t, s.rdb, ev.Payload)
	}
	settleStatusGroup(t, s.rdb)
	assertUnchanged(t, s, hash, docRow{"FAILED", maxAttempts - 1}, maxAttempts)
	assertStamp(t, s, hash, stamp)
}

// stub mode: failed-permanent
func testFailedPermanent(t *testing.T) {
	s := newScenario(t)
	resetStreams(t, s.rdb)

	id := upload(t, "permanent.txt", uniqueContent("failed-permanent"))
	if got := waitFinal(t, id, 60*time.Second); got != "FAILED" {
		t.Fatalf("final status = %s, want FAILED", got)
	}
	hash := s.track(hashOf(t, s.db, id))

	// A permanent failure never consumes a retry and queues no second attempt.
	assertUnchanged(t, s, hash, docRow{"FAILED", 0}, 1)
	events := statusEventsFor(t, s.rdb, hash)
	if len(events) != 1 || events[0].Reason != "CORRUPT_FILE" || events[0].Retryable == nil || *events[0].Retryable {
		t.Fatalf("status events = %+v, want one non-retryable CORRUPT_FILE", events)
	}
}

// stub mode: slow (every attempt overruns job_timeout)
func testSlow(t *testing.T) {
	s := newScenario(t)
	resetStreams(t, s.rdb)

	id := upload(t, "slow.txt", uniqueContent("slow"))
	if got := waitFinal(t, id, 150*time.Second); got != "FAILED" {
		t.Fatalf("final status = %s, want FAILED", got)
	}
	hash := s.track(hashOf(t, s.db, id))

	assertUnchanged(t, s, hash, docRow{"FAILED", maxAttempts - 1}, maxAttempts)
	events := statusEventsFor(t, s.rdb, hash)
	if len(events) != maxAttempts {
		t.Fatalf("status events = %d, want %d: %+v", len(events), maxAttempts, events)
	}
	for _, ev := range events {
		if ev.Reason != "JOB_TIMEOUT" || ev.Retryable == nil || !*ev.Retryable {
			t.Errorf("event %+v, want a retryable JOB_TIMEOUT", ev)
		}
	}
}

// stub mode: crash (the process exits after reading, before publishing or acknowledging)
func testCrash(t *testing.T) {
	assertPoisonedAfterRestarts(t, "crash", 150*time.Second)
}

// stub mode: hang (a step never returns; only the watchdog ends the process)
func testHang(t *testing.T) {
	assertPoisonedAfterRestarts(t, "hang", 180*time.Second)
}

// A consumer that dies mid-job leaves the entry pending. Each restart is a new consumer name; the
// entry is reclaimed after min_idle and, on the 4th delivery, reported as POISON_MESSAGE.
func assertPoisonedAfterRestarts(t *testing.T, label string, timeout time.Duration) {
	s := newScenario(t)
	resetStreams(t, s.rdb)

	watcher := watchConsumers(s.rdb)
	id := upload(t, label+".txt", uniqueContent(label))
	started := time.Now()
	final := waitFinal(t, id, timeout)
	elapsed := time.Since(started)
	names := watcher.finish()
	if final != "FAILED" {
		t.Fatalf("final status = %s, want FAILED", final)
	}
	hash := s.track(hashOf(t, s.db, id))

	// The job never reported anything itself, so the attempt counter did not move.
	assertUnchanged(t, s, hash, docRow{"FAILED", 0}, 1)
	events := statusEventsFor(t, s.rdb, hash)
	if len(events) != 1 {
		t.Fatalf("status events = %+v, want exactly one (the poison report)", events)
	}
	if ev := events[0]; ev.Status != "FAILED" || ev.Reason != "POISON_MESSAGE" || ev.Attempts != 0 || ev.Retryable == nil || *ev.Retryable {
		t.Fatalf("event = %+v, want a non-retryable POISON_MESSAGE echoing attempts=0", ev)
	}
	// 3 process starts die, the 4th reports: every start registered its own consumer name.
	if want := cfg.RAG.Consumer.PoisonThreshold + 1; len(names) != want {
		t.Errorf("saw %d consumer names %v, want exactly %d (one per process start)", len(names), names, want)
	}
	// Each of the three reclaims waits min_idle, so a report much sooner means the poison check
	// fired early.
	if floor := time.Duration(cfg.RAG.Consumer.PoisonThreshold) * cfg.RAG.Consumer.MinIdle.Std(); elapsed < floor {
		t.Errorf("poison reported after %v, want at least %d x min_idle = %v", elapsed, cfg.RAG.Consumer.PoisonThreshold, floor)
	}
}

func testUnsupportedExtension(t *testing.T) {
	for _, filename := range []string{"report.docx", "no-extension", "archive.tar.gz", "photo.png"} {
		resp, err := httpClient.Post(centerURL()+"/upload", "application/json",
			strings.NewReader(`{"filename":"`+filename+`"}`))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "unsupported file extension") {
			t.Errorf("%s: POST /upload = %d %q, want 400 unsupported file extension", filename, resp.StatusCode, body)
		}
	}
}

// Both services delete dead consumers of the group they read, but only those with nothing pending:
// deleting one with pending entries would drop them silently.
//
// The "pending is kept" half runs against the center's status group only. The Python group cannot
// show it here: min_idle (10s) is below consumer_cleanup_idle (15s), so a pending entry is
// reclaimed (and the consumer emptied) before it is old enough to be a cleanup candidate. That
// half is covered by the Python unit and integration tests.
func testConsumerCleanup(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()

	for stream, group := range map[string]string{statusStream: statusGroup, ingestStream: ingestGroup} {
		if err := rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil &&
			!strings.HasPrefix(err.Error(), "BUSYGROUP") {
			t.Fatal(err)
		}
	}
	const idle, pyIdle, holder = "e2e-idle-empty", "e2e-py-idle-empty", "e2e-holder"
	t.Cleanup(func() {
		rdb.XGroupDelConsumer(ctx, statusStream, statusGroup, idle)
		rdb.XGroupDelConsumer(ctx, statusStream, statusGroup, holder)
		rdb.XGroupDelConsumer(ctx, ingestStream, ingestGroup, pyIdle)
	})

	// The holder must be the consumer that reads the entry, not the center's own worker, which
	// is also blocked on this stream. Redis serves blocked readers oldest first and the worker
	// re-issues its 2s block constantly, so a holder that blocked more than 2s earlier is ahead.
	var entryID string
	for attempt := 1; attempt <= 3 && entryID == ""; attempt++ {
		got := make(chan string, 1)
		go func() {
			streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group: statusGroup, Consumer: holder, Streams: []string{statusStream, ">"}, Count: 1, Block: 12 * time.Second,
			}).Result()
			if err == nil && len(streams) > 0 && len(streams[0].Messages) > 0 {
				got <- streams[0].Messages[0].ID
				return
			}
			got <- ""
		}()
		time.Sleep(3 * time.Second)
		// A valid event for a document that does not exist: harmless if the center ever applies it.
		payload, _ := json.Marshal(map[string]any{
			"id": uuid.NewString(), "hash": "e2e-cleanup-" + uuid.NewString(), "status": "INDEXED", "attempts": 0,
			"created_at": time.Now().UTC().Format(time.RFC3339Nano), "schema_version": 1,
		})
		addStatusEntry(t, rdb, string(payload))
		entryID = <-got
		if entryID == "" {
			t.Logf("attempt %d: the center's worker read the entry first; trying again", attempt)
		}
	}
	if entryID == "" {
		t.Fatal("the holder never received an entry (the center's worker kept winning the race)")
	}
	t.Cleanup(func() { rdb.XAck(ctx, statusStream, statusGroup, entryID) })

	// Created only now, so the clock below starts when the holder is already in place.
	if err := rdb.XGroupCreateConsumer(ctx, statusStream, statusGroup, idle).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.XGroupCreateConsumer(ctx, ingestStream, ingestGroup, pyIdle).Err(); err != nil {
		t.Fatal(err)
	}
	created := time.Now()
	goIdleFor := cfg.Go.StatusConsumer.ConsumerCleanupIdle.Std()
	pyIdleFor := cfg.RAG.Consumer.ConsumerCleanupIdle.Std()

	// Too early to go: a consumer idle for less than consumer_cleanup_idle must still be there
	// (a cleanup that ignored the idle time would already have removed it).
	has := func(stream, group, name string) (present bool, pending int64) {
		consumers, err := rdb.XInfoConsumers(ctx, stream, group).Result()
		if err != nil {
			t.Fatalf("XINFO CONSUMERS %s %s: %v", stream, group, err)
		}
		for _, c := range consumers {
			if c.Name == name {
				return true, c.Pending
			}
		}
		return false, 0
	}
	// Check just before the earliest allowed deletion, after at least one cleanup pass.
	time.Sleep(time.Until(created.Add(goIdleFor - time.Second)))
	if ok, _ := has(statusStream, statusGroup, idle); !ok {
		t.Fatalf("the idle consumer was deleted before consumer_cleanup_idle (%v) had passed", goIdleFor)
	}

	// Then it must go, and not before the idle time: 2s recovery passes plus the idle time.
	var goGone, pyGone time.Duration
	if !eventually(pyIdleFor+30*time.Second, func() bool {
		if goGone == 0 {
			if ok, _ := has(statusStream, statusGroup, idle); !ok {
				goGone = time.Since(created)
			}
		}
		if pyGone == 0 {
			if ok, _ := has(ingestStream, ingestGroup, pyIdle); !ok {
				pyGone = time.Since(created)
			}
		}
		return goGone != 0 && pyGone != 0
	}) {
		t.Fatalf("idle consumers with nothing pending were not deleted (center after %v, python after %v)", goGone, pyGone)
	}
	// Redis idle time is measured from creation, so an earlier deletion means the check ignored it.
	if goGone < goIdleFor-500*time.Millisecond {
		t.Errorf("center deleted the idle consumer after %v, before consumer_cleanup_idle %v", goGone, goIdleFor)
	}
	if pyGone < pyIdleFor-500*time.Millisecond {
		t.Errorf("python deleted the idle consumer after %v, before consumer_cleanup_idle %v", pyGone, pyIdleFor)
	}
	if ok, pending := has(statusStream, statusGroup, holder); !ok || pending != 1 {
		t.Fatalf("the consumer holding a pending entry must survive with pending=1 (present=%v pending=%d)", ok, pending)
	}

	// Several cleanup passes later it is still there.
	time.Sleep(3 * cfg.Go.StatusConsumer.RecoveryInterval.Std())
	consumers, err := rdb.XInfoConsumers(ctx, statusStream, statusGroup).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range consumers {
		if c.Name == holder && c.Pending == 1 {
			return
		}
	}
	t.Fatalf("the consumer with a pending entry was deleted after all: %+v", consumers)
}

func assertStamp(t *testing.T, s *scenario, hash, want string) {
	t.Helper()
	if got := updatedAt(t, s.db, hash); got != want {
		t.Fatalf("updated_at moved from %s to %s: the redelivered event modified the row", want, got)
	}
}

// assertUnchanged checks the document row and the number of outbox rows, and that they stay put:
// the caller has just waited for the state to settle.
func assertUnchanged(t *testing.T, s *scenario, hash string, want docRow, wantOutbox int) {
	t.Helper()
	if d := mustDoc(t, s.db, hash); d != want {
		t.Fatalf("document = %+v, want %+v", d, want)
	}
	if n := outboxCount(t, s.db, hash); n != wantOutbox {
		t.Fatalf("outbox rows = %d, want %d", n, wantOutbox)
	}
}
