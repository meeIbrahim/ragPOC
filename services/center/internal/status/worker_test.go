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

	if err := w.readOnce(context.Background()); err != nil {
		t.Fatal(err)
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

	w.recoverOnce(context.Background())

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

	w.recoverOnce(context.Background())

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

	w.recoverOnce(context.Background())

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

	w.recoverOnce(context.Background())

	if src.count("handle:") != 0 || src.count("ack:") != 0 {
		t.Fatalf("entry was touched: %v", src.events())
	}
}

func TestRecoverOnceStillCleansUpWhenClaimFails(t *testing.T) {
	src := &fakeSource{claimFn: func(context.Context, string, time.Duration, int64) ([]events.StreamEntry, error) {
		return nil, errors.New("redis hiccup")
	}}
	w := newWorker(src, recordingHandler(src, func(string) bool { return true }), testConfig())
	w.recoverOnce(context.Background())
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
	w.recoverOnce(context.Background())
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

func TestRunStopsPromptlyWhenContextIsCancelled(t *testing.T) {
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
	if first := src.events()[0]; first != "ensure" {
		t.Fatalf("first call = %s, want ensure", first)
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
