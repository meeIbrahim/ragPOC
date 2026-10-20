package status

import (
	"context"
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
)

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

	for {
		err := w.src.EnsureGroup(ctx)
		if err == nil {
			break
		}
		w.logger.Error("cannot create the status consumer group; retrying", "err", err)
		if !w.sleep(ctx, w.cfg.RetryDelay) {
			return
		}
	}
	w.logger.Info("status worker started")

	var nextRecovery time.Time // zero: recover once right at start
	for ctx.Err() == nil {
		if !w.now().Before(nextRecovery) {
			w.recoverOnce(ctx)
			nextRecovery = w.now().Add(w.cfg.RecoveryInterval)
		}
		if err := w.readOnce(ctx); err != nil && ctx.Err() == nil {
			w.logger.Error("reading the status stream failed; backing off", "err", err)
			w.sleep(ctx, w.cfg.RetryDelay)
		}
	}
}

// readOnce reads one batch of new entries and processes them in order.
func (w *Worker) readOnce(ctx context.Context) error {
	block := min(w.cfg.ReadBlock, w.cfg.RecoveryInterval)
	entries, err := w.src.ReadNew(ctx, w.id, block, w.cfg.BatchSize)
	if err != nil {
		return err
	}
	for _, e := range entries {
		w.process(ctx, e)
	}
	return nil
}

// recoverOnce takes over entries another (dead) consumer left pending, drops the ones that
// have been delivered too often, processes the rest, and deletes consumers that are gone.
func (w *Worker) recoverOnce(ctx context.Context) {
	entries, err := w.src.ClaimStale(ctx, w.id, w.cfg.MinIdle, w.cfg.BatchSize)
	if err != nil {
		w.logger.Error("claiming stale status entries failed", "err", err)
	}
	for _, e := range entries {
		if ctx.Err() != nil {
			return
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
				"threshold", w.cfg.PoisonThreshold, "values", e.Values)
			w.ack(ctx, e.ID)
			continue
		}
		w.process(ctx, e)
	}

	deleted, err := w.src.CleanupConsumers(ctx, w.id, w.cfg.CleanupIdle)
	if err != nil {
		w.logger.Error("cleaning up idle consumers failed", "err", err)
	}
	if len(deleted) > 0 {
		w.logger.Info("deleted idle consumers", "consumers", deleted)
	}
}

func (w *Worker) process(ctx context.Context, e events.StreamEntry) {
	if w.handler.Handle(ctx, e) {
		w.ack(ctx, e.ID)
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
