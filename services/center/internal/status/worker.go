package status

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"

	"center-service/internal/config"
	"center-service/internal/events"
)

const (
	defaultReadBlock  = 2 * time.Second
	defaultBatchSize  = 10
	defaultRetryDelay = time.Second
	ackTimeout        = 5 * time.Second
	// maxRecoveryRounds bounds how many full batches one recovery pass claims.
	maxRecoveryRounds = 10
)

// errEntriesFailed is returned by readOnce when at least one entry of the batch could not be
// finished although the store was healthy. Run backs off before reading again, so a bug in the
// handler cannot make the worker drain the stream at full speed.
var errEntriesFailed = errors.New("some entries could not be finished")

// maxLoggedPoison caps the payload of a dropped entry in the log. Producers control it.
const maxLoggedPoison = 4096

// healthChecker is implemented by handlers that can tell whether the store takes writes.
type healthChecker interface {
	Healthy(ctx context.Context) error
}

// EntryHandler turns one stream entry into a store call and reports whether to acknowledge it.
type EntryHandler interface {
	Handle(ctx context.Context, entry events.StreamEntry) (ack bool)
}

// WorkerConfig tunes the status worker. Build it from config with WorkerConfigFrom.
type WorkerConfig struct {
	// MinIdle is how long an entry must sit pending before the recovery pass takes it over.
	MinIdle time.Duration
	// RecoveryInterval is how often the recovery pass runs.
	RecoveryInterval time.Duration
	// PoisonThreshold: a reclaimed entry delivered more often than this is dropped.
	PoisonThreshold int64
	// CleanupIdle is how long a consumer with no pending entries may sit idle before deletion.
	CleanupIdle time.Duration
	// ReadBlock caps how long one read waits. It is also capped at RecoveryInterval, so the
	// recovery pass is never starved.
	ReadBlock  time.Duration
	BatchSize  int64
	RetryDelay time.Duration
}

// WorkerConfigFrom takes the tunable values from [go.status_consumer].
func WorkerConfigFrom(c config.StatusConsumerConfig) WorkerConfig {
	return WorkerConfig{
		MinIdle:          c.MinIdle.Std(),
		RecoveryInterval: c.RecoveryInterval.Std(),
		PoisonThreshold:  int64(c.PoisonThreshold),
		CleanupIdle:      c.ConsumerCleanupIdle.Std(),
		ReadBlock:        defaultReadBlock,
		BatchSize:        defaultBatchSize,
		RetryDelay:       defaultRetryDelay,
	}
}

// NewWorkerID returns "<hostname>/status/<uuid>". It is new on every process start, so a
// restarted process never drains a previous name's pending list; those entries return only
// through the recovery pass.
func NewWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + "/status/" + uuid.NewString()
}

// Worker reads the status stream, hands entries to the handler and acknowledges the ones that
// are finished. One goroutine runs both the reads and the recovery pass, so a single process
// still recovers its own predecessors' stuck entries.
type Worker struct {
	id      string
	cfg     WorkerConfig
	src     Source
	handler EntryHandler
	now     func() time.Time
	logger  *slog.Logger
}

func NewWorker(id string, cfg WorkerConfig, src Source, handler EntryHandler, logger *slog.Logger) *Worker {
	return &Worker{
		id:      id,
		cfg:     cfg,
		src:     src,
		handler: handler,
		now:     time.Now,
		logger:  logger.With("worker", id),
	}
}

// Run blocks until ctx is cancelled. Redis errors are logged and retried, never fatal.
func (w *Worker) Run(ctx context.Context) {
	defer w.logger.Info("status worker stopped")

	if !w.ensureGroup(ctx) {
		return
	}
	w.logger.Info("status worker started")

	var nextRecovery time.Time // zero: recover once right at start
	for ctx.Err() == nil {
		if !w.now().Before(nextRecovery) {
			err := w.recoverOnce(ctx)
			nextRecovery = w.now().Add(w.cfg.RecoveryInterval)
			if errors.Is(err, events.ErrNoGroup) && !w.recreateGroup(ctx) {
				return
			}
		}

		err := w.readOnce(ctx)
		switch {
		case err == nil || ctx.Err() != nil:
		case errors.Is(err, events.ErrNoGroup):
			if !w.recreateGroup(ctx) {
				return
			}
		case errors.Is(err, errEntriesFailed):
			w.logger.Warn("entries could not be finished and stay pending; backing off")
			w.sleep(ctx, w.cfg.RetryDelay)
		default:
			w.logger.Error("reading the status stream failed; backing off", "err", err)
			w.sleep(ctx, w.cfg.RetryDelay)
		}
	}
}

// ensureGroup retries until the group exists; it reports false when ctx ended first.
func (w *Worker) ensureGroup(ctx context.Context) bool {
	for {
		err := w.src.EnsureGroup(ctx)
		if err == nil {
			return true
		}
		w.logger.Error("cannot create the status consumer group; retrying", "err", err)
		if !w.sleep(ctx, w.cfg.RetryDelay) {
			return false
		}
	}
}

// recreateGroup handles a group that vanished while running (Redis restarted without it in its
// snapshot, the stream was deleted). The group is created at ID 0; replaying older entries is
// safe because the CAS in the store turns stale and duplicate events into no-ops.
func (w *Worker) recreateGroup(ctx context.Context) bool {
	w.logger.Warn("the status consumer group disappeared; creating it again")
	return w.ensureGroup(ctx)
}

// waitUntilHealthy pauses reading after a failed handle until the store can take a write
// again. Reading on would only move the rest of the stream into the pending list, where each
// entry then starts the poison clock for an outage that is not its fault.
func (w *Worker) waitUntilHealthy(ctx context.Context) {
	checker, ok := w.handler.(healthChecker)
	if !ok {
		w.sleep(ctx, w.cfg.RetryDelay)
		return
	}
	logged := false
	for ctx.Err() == nil {
		err := checker.Healthy(ctx)
		if err == nil {
			if logged {
				w.logger.Info("the store accepts writes again; resuming")
			}
			return
		}
		if !logged {
			w.logger.Warn("the store cannot take writes; pausing reads until it can", "err", err)
			logged = true
		}
		w.sleep(ctx, w.cfg.RetryDelay)
	}
}

// readOnce reads one batch of new entries and processes them in order.
//
// XREADGROUP puts the whole batch into this consumer's pending list at once, so abandoning the
// rest of a batch would not leave it unread: the neighbours would come back later, together
// with the failing entry, and be poison-dropped with it. An entry that cannot be finished
// therefore stays pending alone and its neighbours are still handled.
func (w *Worker) readOnce(ctx context.Context) error {
	// go-redis turns a block under 1ms into BLOCK 0, which waits forever.
	block := max(min(w.cfg.ReadBlock, w.cfg.RecoveryInterval), time.Millisecond)
	entries, err := w.src.ReadNew(ctx, w.id, block, w.cfg.BatchSize)
	if err != nil {
		return err
	}
	failed := 0
	for _, e := range entries {
		if ctx.Err() != nil {
			return nil // shutting down: what is left stays pending and is reclaimed later
		}
		if !w.process(ctx, e) {
			failed++
		}
	}
	if failed > 0 {
		return errEntriesFailed
	}
	return nil
}

// recoverOnce takes over entries another (dead) consumer left pending, drops the ones that
// have been delivered too often, processes the rest, and deletes consumers that are gone. It
// returns the claim error so Run can notice a vanished group.
func (w *Worker) recoverOnce(ctx context.Context) error {
	var claimErr error
	if w.storeHealthy(ctx) {
		claimErr = w.claimAndProcess(ctx)
	}

	deleted, err := w.src.CleanupConsumers(ctx, w.id, w.cfg.CleanupIdle)
	if err != nil {
		w.logger.Error("cleaning up idle consumers failed", "err", err)
	}
	if len(deleted) > 0 {
		w.logger.Info("deleted idle consumers", "consumers", deleted)
	}
	return claimErr
}

// storeHealthy gates claiming: a claim bumps the delivery count, and while the store cannot
// take writes that would only push good entries toward the poison threshold.
func (w *Worker) storeHealthy(ctx context.Context) bool {
	checker, ok := w.handler.(healthChecker)
	if !ok {
		return true
	}
	if err := checker.Healthy(ctx); err != nil {
		w.logger.Warn("the store cannot take writes; skipping the recovery claim", "err", err)
		return false
	}
	return true
}

// claimAndProcess claims in rounds while batches come back full (bounded), so a backlog does
// not take one batch per interval to drain.
func (w *Worker) claimAndProcess(ctx context.Context) error {
	for round := 0; round < maxRecoveryRounds; round++ {
		entries, err := w.src.ClaimStale(ctx, w.id, w.cfg.MinIdle, w.cfg.BatchSize)
		if err != nil {
			w.logger.Error("claiming stale status entries failed", "err", err)
			return err
		}
		for _, e := range entries {
			if ctx.Err() != nil {
				return nil
			}
			deliveries, err := w.src.DeliveryCount(ctx, e.ID)
			if err != nil {
				// Neither ack (it may be a good event) nor handle (it may be poison): the entry
				// stays pending and is reclaimed on a later pass.
				w.logger.Error("reading the delivery count failed; leaving the entry pending", "entry", e.ID, "err", err)
				continue
			}
			if deliveries > w.cfg.PoisonThreshold {
				// The center cannot publish a FAILED event for itself; the document stays
				// PROCESSING until the reconciliation sweep (backlog B2) repairs it.
				w.logger.Error("dropping poison status entry", "entry", e.ID, "deliveries", deliveries,
					"threshold", w.cfg.PoisonThreshold, "values", clipValues(e.Values))
				w.ack(ctx, e.ID)
				continue
			}
			w.process(ctx, e) // a failing entry stays pending alone; its neighbours still run
		}
		if int64(len(entries)) < w.cfg.BatchSize {
			return nil
		}
	}
	return nil
}

// process hands the entry to the handler and acknowledges it when it is finished. It reports
// whether the entry was finished.
//
// When the handler fails, the store's health decides what happens next. Handle fails only on a
// database error, so the failure is most likely the store's:
//   - If the store cannot take writes, wait until it can and try the same entry again in place
//     (it was already delivered, so abandoning it would only start its poison clock).
//   - If the store looks fine, the cause may already have cleared (a lock released between the
//     failure and the probe), so try once more before blaming the entry. An entry that still
//     fails is left pending alone and the caller moves on.
//
// Every retry is preceded by a RetryDelay sleep, so a store that flips between failing and
// passing its probe cannot turn this into a tight loop.
func (w *Worker) process(ctx context.Context, e events.StreamEntry) bool {
	retriedWhileHealthy := false
	for {
		if w.handler.Handle(ctx, e) {
			w.ack(ctx, e.ID)
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		checker, ok := w.handler.(healthChecker)
		if !ok {
			return false // cannot tell a store problem from a bad entry
		}
		if checker.Healthy(ctx) != nil {
			w.waitUntilHealthy(ctx)
		} else {
			if retriedWhileHealthy {
				return false
			}
			retriedWhileHealthy = true
		}
		if !w.sleep(ctx, w.cfg.RetryDelay) {
			return false
		}
	}
}

// ack runs on a context that survives shutdown: the entry is already handled, and leaving it
// unacked would only redeliver it after min_idle.
func (w *Worker) ack(ctx context.Context, id string) {
	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()
	if err := w.src.Ack(ackCtx, id); err != nil {
		w.logger.Error("ack failed; the entry will be redelivered", "entry", id, "err", err)
	}
}

// sleep waits for d or until ctx is cancelled; it reports whether the full wait happened.
func (w *Worker) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func clipValues(values map[string]any) map[string]any {
	clipped := make(map[string]any, len(values))
	for k, v := range values {
		if text, ok := v.(string); ok && len(text) > maxLoggedPoison {
			v = text[:maxLoggedPoison] + "..."
		}
		clipped[k] = v
	}
	return clipped
}
