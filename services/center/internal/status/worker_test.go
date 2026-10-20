package status

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"center-service/internal/events"
)

// fakeSource records every call in order and lets each test script the behaviour it needs.
type fakeSource struct {
	mu  sync.Mutex
	log []string

	ensureFn  func(ctx context.Context) error
	readFn    func(ctx context.Context, consumer string, block time.Duration, count int64) ([]events.StreamEntry, error)
	claimFn   func(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]events.StreamEntry, error)
	countFn   func(ctx context.Context, id string) (int64, error)
	ackFn     func(ctx context.Context, id string) error
	cleanupFn func(ctx context.Context, self string, idle time.Duration) ([]string, error)

	blocks []time.Duration
}

func (f *fakeSource) record(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = append(f.log, fmt.Sprintf(format, args...))
}

func (f *fakeSource) events() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.log...)
}

func (f *fakeSource) count(prefix string) int {
	n := 0
	for _, e := range f.events() {
		if strings.HasPrefix(e, prefix) {
			n++
		}
	}
	return n
}

func (f *fakeSource) EnsureGroup(ctx context.Context) error {
	f.record("ensure")
	if f.ensureFn != nil {
		return f.ensureFn(ctx)
	}
	return nil
}

func (f *fakeSource) ReadNew(ctx context.Context, consumer string, block time.Duration, count int64) ([]events.StreamEntry, error) {
	f.mu.Lock()
	f.blocks = append(f.blocks, block)
	f.mu.Unlock()
	f.record("read")
	if f.readFn != nil {
		return f.readFn(ctx, consumer, block, count)
	}
	select { // nothing to read: behave like a blocking read that times out
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Millisecond):
		return nil, nil
	}
}

func (f *fakeSource) ClaimStale(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]events.StreamEntry, error) {
	f.record("claim:%s:%v", consumer, minIdle)
	if f.claimFn != nil {
		return f.claimFn(ctx, consumer, minIdle, count)
	}
	return nil, nil
}

func (f *fakeSource) DeliveryCount(ctx context.Context, id string) (int64, error) {
	f.record("count:%s", id)
	if f.countFn != nil {
		return f.countFn(ctx, id)
	}
	return 1, nil
}

func (f *fakeSource) Ack(ctx context.Context, id string) error {
	f.record("ack:%s", id)
	if f.ackFn != nil {
		return f.ackFn(ctx, id)
	}
	return nil
}

func (f *fakeSource) CleanupConsumers(ctx context.Context, self string, idle time.Duration) ([]string, error) {
	f.record("cleanup:%s:%v", self, idle)
	if f.cleanupFn != nil {
		return f.cleanupFn(ctx, self, idle)
	}
	return nil, nil
}

type handlerFunc func(ctx context.Context, e events.StreamEntry) bool

func (h handlerFunc) Handle(ctx context.Context, e events.StreamEntry) bool { return h(ctx, e) }

// healthHandler adds the optional store health probe the worker uses to avoid reading or
// claiming while the database cannot take writes.
type healthHandler struct {
	handlerFunc
	healthy func(ctx context.Context) error
}

func (h healthHandler) Healthy(ctx context.Context) error { return h.healthy(ctx) }

func entry(id string) events.StreamEntry { return events.StreamEntry{ID: id} }

func testConfig() WorkerConfig {
	return WorkerConfig{
		MinIdle:          time.Minute,
		RecoveryInterval: time.Hour, // tests drive the recovery pass directly unless they say otherwise
		PoisonThreshold:  3,
		CleanupIdle:      time.Hour,
		ReadBlock:        time.Second,
		BatchSize:        10,
		RetryDelay:       time.Millisecond,
	}
}

func newWorker(src Source, h EntryHandler, cfg WorkerConfig) *Worker {
	return NewWorker("host/status/test", cfg, src, h, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// recordingHandler logs into the source's event list so call order across both is visible.
func recordingHandler(src *fakeSource, ack func(id string) bool) EntryHandler {
	return handlerFunc(func(ctx context.Context, e events.StreamEntry) bool {
		src.record("handle:%s", e.ID)
		return ack(e.ID)
	})
}

func TestWorkerIDHasKindAndIsUniquePerCall(t *testing.T) {
	re := regexp.MustCompile(`^.+/status/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	a, b := NewWorkerID(), NewWorkerID()
	if !re.MatchString(a) || !re.MatchString(b) {
		t.Fatalf("ids %q, %q do not match <host>/status/<uuid>", a, b)
	}
	if a == b {
		t.Fatal("two ids are equal; a restarted process must get a new name")
	}
}

// Ack order (L17): the entry is handled first and acknowledged only afterwards, and only when
// the handler says it is finished.
func TestReadOnceAcksOnlyAfterHandlingAndOnlyWhenHandled(t *testing.T) {
	src := &fakeSource{}
	src.readFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return []events.StreamEntry{entry("1-0"), entry("2-0"), entry("3-0")}, nil
	}
	w := newWorker(src, recordingHandler(src, func(id string) bool { return id != "2-0" }), testConfig())

	// 2-0 cannot be finished and stays pending; its neighbours are still handled.
	if err := w.readOnce(context.Background()); !errors.Is(err, errEntriesFailed) {
		t.Fatalf("err = %v, want errEntriesFailed", err)
	}
	want := "read,handle:1-0,ack:1-0,handle:2-0,handle:3-0,ack:3-0"
	if got := strings.Join(src.events(), ","); got != want {
		t.Fatalf("calls = %s\nwant    %s", got, want)
	}
}

func TestReadOnceReturnsSourceErrors(t *testing.T) {
	src := &fakeSource{readFn: func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return nil, errors.New("connection refused")
	}}
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), testConfig())
	if err := w.readOnce(context.Background()); err == nil {
		t.Fatal("expected the read error to be returned for backoff")
	}
}

func TestReadOnceUsesWorkerIDAndBatchSize(t *testing.T) {
	src := &fakeSource{}
	var gotConsumer string
	var gotCount int64
	src.readFn = func(_ context.Context, consumer string, _ time.Duration, count int64) ([]events.StreamEntry, error) {
		gotConsumer, gotCount = consumer, count
		return nil, nil
	}
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), testConfig())
	if err := w.readOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if gotConsumer != "host/status/test" || gotCount != 10 {
		t.Fatalf("consumer=%q count=%d", gotConsumer, gotCount)
	}
}

// The read must return in time for the recovery pass, or a single-consumer deployment would
// never recover (PRD 5.3).
func TestReadBlockNeverExceedsRecoveryInterval(t *testing.T) {
	src := &fakeSource{}
	cfg := testConfig()
	cfg.ReadBlock, cfg.RecoveryInterval = 10*time.Second, 2*time.Second
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), cfg)
	if err := w.readOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(src.blocks) != 1 || src.blocks[0] != 2*time.Second {
		t.Fatalf("block = %v, want 2s", src.blocks)
	}
}

// After the handler finishes, a shutdown signal must not stop the ack from reaching Redis:
// an unacked finished entry would be redelivered after min_idle for no reason.
func TestAckSurvivesCancellationOfTheRunContext(t *testing.T) {
	src := &fakeSource{}
	ctx, cancel := context.WithCancel(context.Background())
	var ackCtxErr error
	src.ackFn = func(ackCtx context.Context, _ string) error {
		ackCtxErr = ackCtx.Err()
		return nil
	}
	src.readFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return []events.StreamEntry{entry("1-0")}, nil
	}
	w := newWorker(src, handlerFunc(func(context.Context, events.StreamEntry) bool {
		cancel() // SIGTERM arrives while the entry is being handled
		return true
	}), testConfig())

	if err := w.readOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if src.count("ack:1-0") != 1 {
		t.Fatalf("entry not acked: %v", src.events())
	}
	if ackCtxErr != nil {
		t.Fatalf("ack ran with a cancelled context: %v", ackCtxErr)
	}
}

func TestAckErrorIsLoggedNotFatal(t *testing.T) {
	src := &fakeSource{ackFn: func(context.Context, string) error { return errors.New("redis down") }}
	src.readFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return []events.StreamEntry{entry("1-0"), entry("2-0")}, nil
	}
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), testConfig())
	if err := w.readOnce(context.Background()); err != nil {
		t.Fatalf("an ack failure must not abort the batch: %v", err)
	}
	if src.count("handle:") != 2 {
		t.Fatalf("second entry not handled: %v", src.events())
	}
}

// ---- recovery pass

func TestRecoverOnceProcessesReclaimedEntriesUnderThePoisonThreshold(t *testing.T) {
	src := &fakeSource{}
	src.claimFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return []events.StreamEntry{entry("1-0")}, nil
	}
	src.countFn = func(context.Context, string) (int64, error) { return 3, nil } // 3 is not over 3
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), testConfig())

	_ = w.recoverOnce(context.Background())

	want := "claim:host/status/test:1m0s,count:1-0,handle:1-0,ack:1-0,cleanup:host/status/test:1h0m0s"
	if got := strings.Join(src.events(), ","); got != want {
		t.Fatalf("calls = %s\nwant    %s", got, want)
	}
}

// A reclaimed entry delivered more than the threshold is dropped: logged in full and acked,
// never handled again.
func TestRecoverOnceDropsPoisonEntries(t *testing.T) {
	src := &fakeSource{}
	src.claimFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return []events.StreamEntry{{ID: "9-0", Values: map[string]any{"payload": "p"}}}, nil
	}
	src.countFn = func(context.Context, string) (int64, error) { return 4, nil }
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), testConfig())

	_ = w.recoverOnce(context.Background())

	if src.count("handle:") != 0 {
		t.Fatal("a poison entry was handled again")
	}
	if src.count("ack:9-0") != 1 {
		t.Fatalf("poison entry not acked: %v", src.events())
	}
}

func TestRecoverOnceLogsTheFullPoisonEntry(t *testing.T) {
	src := &fakeSource{}
	src.claimFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return []events.StreamEntry{{ID: "9-0", Values: map[string]any{"payload": "the-full-payload"}}}, nil
	}
	src.countFn = func(context.Context, string) (int64, error) { return 4, nil }
	var logs strings.Builder
	w := NewWorker("host/status/test", testConfig(), src, recordingHandler(src, func(string) bool { return true }),
		slog.New(slog.NewTextHandler(&logs, nil)))

	_ = w.recoverOnce(context.Background())

	for _, want := range []string{"level=ERROR", "9-0", "the-full-payload"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log lacks %q: %s", want, logs.String())
		}
	}
}

// If the delivery count cannot be read the entry is left pending: acking could drop a good
// event, and handling it blindly could loop a poison one.
func TestRecoverOnceLeavesEntryPendingWhenCountFails(t *testing.T) {
	src := &fakeSource{}
	src.claimFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return []events.StreamEntry{entry("1-0")}, nil
	}
	src.countFn = func(context.Context, string) (int64, error) { return 0, errors.New("redis down") }
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), testConfig())

	_ = w.recoverOnce(context.Background())

	if src.count("handle:") != 0 || src.count("ack:") != 0 {
		t.Fatalf("entry was touched: %v", src.events())
	}
}

func TestRecoverOnceStillCleansUpWhenClaimFails(t *testing.T) {
	src := &fakeSource{claimFn: func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return nil, errors.New("redis hiccup")
	}}
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), testConfig())
	_ = w.recoverOnce(context.Background())
	if src.count("cleanup:host/status/test:1h0m0s") != 1 {
		t.Fatalf("cleanup skipped: %v", src.events())
	}
}

func TestRecoverOnceKeepsEntryPendingWhenHandlerFails(t *testing.T) {
	src := &fakeSource{}
	src.claimFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return []events.StreamEntry{entry("1-0")}, nil
	}
	w := newWorker(src, recordingHandler(src, func(string) bool { return false }), testConfig())
	_ = w.recoverOnce(context.Background())
	if src.count("ack:") != 0 {
		t.Fatal("acked although the handler failed")
	}
}

// ---- Run

func TestRunRetriesEnsureGroupUntilItWorks(t *testing.T) {
	src := &fakeSource{}
	calls := 0
	src.ensureFn = func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("redis not ready")
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	src.readFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		cancel() // the group exists and the loop reached its first read
		return nil, nil
	}
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), testConfig())

	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	waitFor(t, done)

	if src.count("ensure") != 3 {
		t.Fatalf("EnsureGroup called %d times, want 3", src.count("ensure"))
	}
	if src.count("read") < 1 {
		t.Fatal("the loop never started reading")
	}
}

// Redis errors are survivable: log, back off, keep going (spec E10).
func TestRunSurvivesReadErrors(t *testing.T) {
	src := &fakeSource{}
	reads := 0
	ctx, cancel := context.WithCancel(context.Background())
	src.readFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		reads++
		switch reads {
		case 1:
			return nil, errors.New("connection reset")
		case 2:
			return []events.StreamEntry{entry("1-0")}, nil
		}
		cancel()
		return nil, nil
	}
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), testConfig())

	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	waitFor(t, done)

	if src.count("ack:1-0") != 1 {
		t.Fatalf("the entry after the failed read was not processed: %v", src.events())
	}
}

func TestRunReturnsOnceTheContextIsCancelledAndReadReturns(t *testing.T) {
	src := &fakeSource{}
	ctx, cancel := context.WithCancel(context.Background())
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), testConfig())

	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	waitFor(t, done)
}

func TestRunStopsWhileWaitingForTheGroup(t *testing.T) {
	src := &fakeSource{ensureFn: func(context.Context) error { return errors.New("never ready") }}
	cfg := testConfig()
	cfg.RetryDelay = time.Hour // the stop must interrupt the backoff sleep
	ctx, cancel := context.WithCancel(context.Background())
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), cfg)

	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	waitFor(t, done)
}

// Recovery runs once at start and then every RecoveryInterval, interleaved with reads.
func TestRunRecoversPeriodically(t *testing.T) {
	src := &fakeSource{}
	cfg := testConfig()
	cfg.RecoveryInterval = 30 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), cfg)

	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	time.Sleep(200 * time.Millisecond)
	cancel()
	waitFor(t, done)

	if n := src.count("claim:"); n < 3 {
		t.Fatalf("recovery ran %d times in 200ms at a 30ms interval, want at least 3", n)
	}
	if got := src.events(); got[0] != "ensure" || !strings.HasPrefix(got[1], "claim:") {
		t.Fatalf("first calls = %v, want ensure then an immediate recovery pass", got[:2])
	}
	if n := src.count("cleanup:"); n != src.count("claim:") {
		t.Fatalf("cleanup ran %d times for %d claims; each pass does both", n, src.count("claim:"))
	}
}

func waitFor(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return")
	}
}

// ---- review round: group loss, store outages, shutdown, claim rounds

func noGroup() error { return fmt.Errorf("%w: NOGROUP No such key", events.ErrNoGroup) }

// If Redis loses the group (restart before a snapshot, FLUSHALL) every read fails with NOGROUP.
// The worker must create it again, or every later status event is ignored.
func TestRunRecreatesAGroupThatDisappeared(t *testing.T) {
	src := &fakeSource{}
	reads := 0
	ctx, cancel := context.WithCancel(context.Background())
	src.readFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		reads++
		switch reads {
		case 1:
			return nil, noGroup()
		case 2:
			return []events.StreamEntry{entry("1-0")}, nil
		}
		cancel()
		return nil, nil
	}
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), testConfig())

	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	waitFor(t, done)

	if src.count("ensure") != 2 {
		t.Fatalf("EnsureGroup called %d times, want 2 (start + after NOGROUP): %v", src.count("ensure"), src.events())
	}
	if src.count("ack:1-0") != 1 {
		t.Fatalf("entry after the group was recreated was not processed: %v", src.events())
	}
}

func TestRunRecreatesAGroupThatDisappearedDuringRecovery(t *testing.T) {
	src := &fakeSource{}
	claims := 0
	ctx, cancel := context.WithCancel(context.Background())
	src.claimFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		claims++
		return nil, noGroup()
	}
	src.readFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		cancel() // the loop got past the recovery pass and the group re-creation
		return nil, nil
	}
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), testConfig())

	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	waitFor(t, done)

	if src.count("ensure") < 2 {
		t.Fatalf("EnsureGroup not repeated after NOGROUP in the recovery pass: %v", src.events())
	}
}

// XREADGROUP delivers the whole batch into the pending list at once, so abandoning the rest of
// a batch does not leave it "unread": the neighbours would come back with the failing entry and
// be poison-dropped with it. A failing entry must not hold its neighbours back.
func TestReadOnceHandlesTheNeighboursOfAFailingEntry(t *testing.T) {
	src := &fakeSource{}
	src.readFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return []events.StreamEntry{entry("1-0"), entry("2-0"), entry("3-0"), entry("4-0")}, nil
	}
	h := healthHandler{
		handlerFunc: func(_ context.Context, e events.StreamEntry) bool {
			src.record("handle:%s", e.ID)
			return e.ID != "1-0" // the head entry is bad
		},
		healthy: func(context.Context) error { return nil }, // the store is fine: the entry is the problem
	}
	w := newWorker(src, h, testConfig())

	err := w.readOnce(context.Background())
	if !errors.Is(err, errEntriesFailed) {
		t.Fatalf("err = %v, want errEntriesFailed so Run backs off", err)
	}
	for _, id := range []string{"2-0", "3-0", "4-0"} {
		if src.count("ack:"+id) != 1 {
			t.Errorf("neighbour %s was not handled and acked: %v", id, src.events())
		}
	}
	if src.count("ack:1-0") != 0 {
		t.Error("the failing entry was acked")
	}
	if src.count("handle:1-0") != 1 {
		t.Errorf("the failing entry was retried in place although the store is healthy: %v", src.events())
	}
}

// When the store is the cause, the same entry is retried in place after it recovers: it was
// already delivered, so it must not be abandoned to the poison clock.
func TestFailedEntryIsRetriedInPlaceOnceTheStoreRecovers(t *testing.T) {
	src := &fakeSource{}
	src.readFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return []events.StreamEntry{entry("1-0"), entry("2-0")}, nil
	}
	attempts := 0
	probes := 0
	h := healthHandler{
		handlerFunc: func(_ context.Context, e events.StreamEntry) bool {
			src.record("handle:%s", e.ID)
			if e.ID == "1-0" {
				attempts++
				return attempts > 1 // fails once, while the store is down
			}
			return true
		},
		healthy: func(context.Context) error {
			probes++
			if probes <= 3 {
				return errors.New("database is locked")
			}
			return nil
		},
	}
	w := newWorker(src, h, testConfig())

	if err := w.readOnce(context.Background()); err != nil {
		t.Fatalf("err = %v, want nil: the entry finished after the store recovered", err)
	}
	want := "read,handle:1-0,handle:1-0,ack:1-0,handle:2-0,ack:2-0"
	if got := strings.Join(src.events(), ","); got != want {
		t.Fatalf("calls = %s\nwant    %s", got, want)
	}
}

// While the store is unhealthy no new entries are read, so an outage cannot drain the stream
// into the pending list.
func TestRunDoesNotReadWhileTheStoreIsUnhealthy(t *testing.T) {
	src := &fakeSource{}
	ctx, cancel := context.WithCancel(context.Background())
	reads := 0
	src.readFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		reads++
		if reads == 1 {
			return []events.StreamEntry{entry("1-0")}, nil
		}
		cancel()
		return nil, nil
	}
	var probes int
	handles := 0
	h := healthHandler{
		handlerFunc: func(context.Context, events.StreamEntry) bool { handles++; return false },
		healthy: func(context.Context) error {
			probes++
			if probes < 4 {
				return errors.New("database is locked")
			}
			return nil
		},
	}
	w := newWorker(src, h, testConfig())

	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	waitFor(t, done)

	if probes < 4 {
		t.Fatalf("only %d probes; the worker did not wait for the store", probes)
	}
	if reads != 2 {
		t.Fatalf("%d reads: one failed batch plus one after recovery expected", reads)
	}
	if handles != 2 {
		t.Fatalf("entry handled %d times, want 2: once, then retried in place after the store recovered", handles)
	}
}

// Claiming bumps the delivery count. While the store is down that would push good entries
// toward the poison threshold for no reason, so the pass skips the claim.
func TestRecoverOnceSkipsClaimingWhileTheStoreIsUnhealthy(t *testing.T) {
	src := &fakeSource{}
	h := healthHandler{
		handlerFunc: func(context.Context, events.StreamEntry) bool { return true },
		healthy:     func(context.Context) error { return errors.New("disk full") },
	}
	w := newWorker(src, h, testConfig())

	if err := w.recoverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if src.count("claim:") != 0 {
		t.Fatalf("claimed while unhealthy: %v", src.events())
	}
	if src.count("cleanup:") != 1 {
		t.Fatalf("cleanup should still run (it needs no database): %v", src.events())
	}
}

// A full batch means more stale entries may be waiting; one pass keeps claiming (bounded) so a
// backlog does not take hours to drain at one batch per interval.
func TestRecoverOnceClaimsInRoundsWhileBatchesAreFull(t *testing.T) {
	src := &fakeSource{}
	cfg := testConfig()
	cfg.BatchSize = 2
	round := 0
	src.claimFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		round++
		switch round {
		case 1:
			return []events.StreamEntry{entry("1-0"), entry("2-0")}, nil
		case 2:
			return []events.StreamEntry{entry("3-0"), entry("4-0")}, nil
		}
		return []events.StreamEntry{entry("5-0")}, nil // partial batch: stop
	}
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), cfg)

	if err := w.recoverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if src.count("claim:") != 3 || src.count("handle:") != 5 {
		t.Fatalf("claims=%d handles=%d, want 3 and 5: %v", src.count("claim:"), src.count("handle:"), src.events())
	}
}

func TestRecoverOnceRoundsAreBounded(t *testing.T) {
	src := &fakeSource{}
	cfg := testConfig()
	cfg.BatchSize = 1
	src.claimFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return []events.StreamEntry{entry("1-0")}, nil // always full
	}
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), cfg)
	_ = w.recoverOnce(context.Background())
	if n := src.count("claim:"); n != maxRecoveryRounds {
		t.Fatalf("claimed %d rounds, want the bound %d", n, maxRecoveryRounds)
	}
}

func TestReadOnceStopsProcessingTheRestOfTheBatchOnShutdown(t *testing.T) {
	src := &fakeSource{}
	ctx, cancel := context.WithCancel(context.Background())
	src.readFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return []events.StreamEntry{entry("1-0"), entry("2-0"), entry("3-0")}, nil
	}
	w := newWorker(src, handlerFunc(func(_ context.Context, e events.StreamEntry) bool {
		src.record("handle:%s", e.ID)
		cancel()
		return true
	}), testConfig())

	if err := w.readOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if src.count("handle:") != 1 || src.count("ack:1-0") != 1 {
		t.Fatalf("calls = %v, want the in-flight entry finished and the rest left for later", src.events())
	}
}

// go-redis turns a block under 1ms into BLOCK 0, which waits forever.
func TestReadBlockIsAtLeastOneMillisecond(t *testing.T) {
	src := &fakeSource{}
	cfg := testConfig()
	cfg.RecoveryInterval = 500 * time.Microsecond
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), cfg)
	if err := w.readOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(src.blocks) != 1 || src.blocks[0] < time.Millisecond {
		t.Fatalf("block = %v, want at least 1ms", src.blocks)
	}
}

func TestRunBacksOffAfterAReadError(t *testing.T) {
	src := &fakeSource{}
	cfg := testConfig()
	cfg.RetryDelay = 80 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	var times []time.Time
	src.readFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		times = append(times, time.Now())
		if len(times) == 1 {
			return nil, errors.New("connection reset")
		}
		cancel()
		return nil, nil
	}
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), cfg)

	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	waitFor(t, done)

	if len(times) < 2 {
		t.Fatalf("only %d reads happened", len(times))
	}
	if gap := times[1].Sub(times[0]); gap < 70*time.Millisecond {
		t.Fatalf("second read came %v after the failed one, want the retry delay", gap)
	}
}

// The same holds for claimed entries: XAUTOCLAIM bumps the delivery count of the whole batch,
// so a failing head entry must not stop its neighbours from being handled before the poison
// threshold reaches all of them together.
func TestRecoverOnceHandlesTheNeighboursOfAFailingClaimedEntry(t *testing.T) {
	src := &fakeSource{}
	src.claimFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return []events.StreamEntry{entry("1-0"), entry("2-0"), entry("3-0")}, nil
	}
	h := healthHandler{
		handlerFunc: func(_ context.Context, e events.StreamEntry) bool {
			src.record("handle:%s", e.ID)
			return e.ID != "1-0"
		},
		healthy: func(context.Context) error { return nil },
	}
	w := newWorker(src, h, testConfig())

	if err := w.recoverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if src.count("ack:2-0") != 1 || src.count("ack:3-0") != 1 || src.count("ack:1-0") != 0 {
		t.Fatalf("calls = %v, want 2-0 and 3-0 acked and 1-0 left pending", src.events())
	}
}

// Failures while the store is healthy must not let a bug drain the stream at full speed.
func TestRunBacksOffWhenEntriesFailWhileTheStoreIsHealthy(t *testing.T) {
	src := &fakeSource{}
	cfg := testConfig()
	cfg.RetryDelay = 80 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	var times []time.Time
	src.readFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		times = append(times, time.Now())
		if len(times) == 1 {
			return []events.StreamEntry{entry("1-0")}, nil
		}
		cancel()
		return nil, nil
	}
	h := healthHandler{
		handlerFunc: func(context.Context, events.StreamEntry) bool { return false },
		healthy:     func(context.Context) error { return nil },
	}
	w := newWorker(src, h, cfg)

	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	waitFor(t, done)

	if len(times) < 2 {
		t.Fatalf("only %d reads", len(times))
	}
	if gap := times[1].Sub(times[0]); gap < 70*time.Millisecond {
		t.Fatalf("next read came %v after a batch with a failing entry, want the retry delay", gap)
	}
}

func TestPoisonLogClipsAHugePayload(t *testing.T) {
	src := &fakeSource{}
	src.claimFn = func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return []events.StreamEntry{{ID: "9-0", Values: map[string]any{"payload": strings.Repeat("x", 500000)}}}, nil
	}
	src.countFn = func(context.Context, string) (int64, error) { return 4, nil }
	var logs strings.Builder
	w := NewWorker("host/status/test", testConfig(), src, recordingHandler(src, func(string) bool { return true }),
		slog.New(slog.NewTextHandler(&logs, nil)))

	_ = w.recoverOnce(context.Background())

	if logs.Len() > 10000 {
		t.Fatalf("the poison log line is %d bytes; a producer-controlled payload must be clipped", logs.Len())
	}
	if !strings.Contains(logs.String(), "9-0") {
		t.Fatal("the poison log lost the entry id")
	}
}
