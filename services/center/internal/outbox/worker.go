package outbox

import (
	"center-service/internal/domain"
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
)

type Store interface {
	FetchCandidates(ctx context.Context, limit int) ([]*domain.OutboxMessage, error)
	ClaimOutboxMessage(ctx context.Context, claimedBy string, id int64) (bool, error)
	MarkPublished(ctx context.Context, id int64, claimedBy string) (bool, error)
	RecoverStaleOutbox(ctx context.Context, cutoff time.Time, maxRetries int) (int64, error)
	GetDocumentByHash(ctx context.Context, hash string) (*domain.Document, error)
	ReleaseClaim(ctx context.Context, id int64, claimedBy string, maxRetries int) (bool, error)
	MarkBlocked(ctx context.Context, id int64, claimedBy string) (bool, error)
}

type Config struct {
	Interval         time.Duration
	BatchSize        int
	JobTimeout       time.Duration
	MaxRetries       int
	RecoveryInterval time.Duration
	StaleThreshold   time.Duration
	SchemaVersion    int
}

func DefaultConfig() Config {
	return Config{
		Interval:         5 * time.Second,
		BatchSize:        100,
		JobTimeout:       30 * time.Second,
		MaxRetries:       3,
		RecoveryInterval: 1 * time.Minute,
		StaleThreshold:   30 * time.Minute,
		SchemaVersion:    1,
	}
}

func NewWorkerId() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return host + "/outbox/" + uuid.NewString()
}

type Worker struct {
	id     string
	cfg    Config
	repo   Store
	logger *slog.Logger
}

func NewWorker(id string, cfg Config, repo Store, logger *slog.Logger) *Worker {
	return &Worker{
		id:     id,
		cfg:    cfg,
		repo:   repo,
		logger: logger.With("worker", id),
	}
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Worker stopped")
			return
		case <-ticker.C:
			w.pass(ctx)
		}
	}
}

func (w *Worker) pass(ctx context.Context) {
	messages, err := w.repo.FetchCandidates(ctx, w.cfg.BatchSize)
	if err != nil {
		w.logger.Error("Failed to fetch candidates", "error", err)
		return
	}
	for _, msg := range messages {
		if ctx.Err() != nil {
			w.logger.Info("Context canceled, stopping processing")
			return
		}
		claimed, err := w.repo.ClaimOutboxMessage(ctx, w.id, msg.Id)
		if err != nil {
			w.logger.Error("Failed to claim outbox message", "id", msg.Id, "error", err)
			continue
		}
		if !claimed {
			continue // another worker claimed it
		}
		w.processMessage(ctx, msg)
	}
}

var (
	ErrDocumentNotFound = errors.New("document not found")
	ErrLostClaim        = errors.New("lost claim")
)

func (w *Worker) processMessage(ctx context.Context, msg *domain.OutboxMessage) {
	jobCtx, cancel := context.WithTimeout(ctx, w.cfg.JobTimeout)
	defer cancel()
	document, err := w.repo.GetDocumentByHash(jobCtx, msg.Hash)
	if err != nil {
		w.logger.Error("Failed to get document by hash", "hash", msg.Hash, "error", err)
		w.releaseClaim(ctx, msg.Id)
		return
	}
	if document == nil {
		w.logger.Error("Document not found for hash", "hash", msg.Hash)
		w.markBlocked(ctx, msg.Id)
		return
	}
	err = w.publishMessage(jobCtx, msg, document)
	if err != nil {
		w.logger.Error("Failed to publish message", "id", msg.Id, "error", err)
		w.releaseClaim(ctx, msg.Id)
		return
	}
	success, err := w.repo.MarkPublished(ctx, msg.Id, w.id)
	if err != nil {
		w.logger.Error("Failed to update message after successful publish", "id", msg.Id, "hash", msg.Hash, "error", err)
		return
	}
	if !success {
		w.logger.Warn("Failed to mark message as published, possibly lost claim", "id", msg.Id, "hash", msg.Hash)
	}
}

func (w *Worker) publishMessage(ctx context.Context, msg *domain.OutboxMessage, document *domain.Document) error {
	// TODO: Publish Logic
	// event := Event{
	// 	Hash:          msg.Hash,
	// 	BucketPath:    document.PermBucketPath,
	// 	FileName:      document.FileName,
	// 	Size:          document.Size,
	// 	Attempt:       document.Retries,
	// 	SchemaVersion: w.cfg.SchemaVersion,
	// }
	return nil
}

func (w *Worker) markBlocked(ctx context.Context, id int64) {
	success, err := w.repo.MarkBlocked(ctx, id, w.id)
	if err != nil {
		w.logger.Error("Failed to mark message as blocked", "id", id, "error", err)
		return
	}
	if !success {
		w.logger.Warn("Failed to mark message as blocked, possibly lost claim", "id", id)
	}

}
func (w *Worker) releaseClaim(ctx context.Context, id int64) {
	success, err := w.repo.ReleaseClaim(ctx, id, w.id, w.cfg.MaxRetries)
	if err != nil {
		w.logger.Error("Failed to release claim", "id", id, "error", err)
		return
	}
	if !success {
		w.logger.Warn("Failed to release claim, possibly lost claim", "id", id)
	}
}

type Sweeper struct {
	cfg    Config
	repo   Store
	logger *slog.Logger
}

func NewSweeper(cfg Config, repo Store, logger *slog.Logger) *Sweeper {
	return &Sweeper{
		cfg:    cfg,
		repo:   repo,
		logger: logger.With("component", "outbox-recovery"),
	}
}

func (s *Sweeper) Run(ctx context.Context) {
	//Recover
}
