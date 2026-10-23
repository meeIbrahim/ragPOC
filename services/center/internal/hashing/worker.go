package hashing

import (
	"center-service/internal/domain"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
)

// Store is what the hash worker and recovery sweeper need from persistence.
type Store interface {
	GetIntentsForHashing(ctx context.Context, limit int) ([]*domain.Intent, error)
	UpdateIntentToHashing(ctx context.Context, id string, claimedBy *string, claimedAt *time.Time) (int64, error)
	MarkIntentFailed(ctx context.Context, id string, claimedBy string) (int64, error)
	ReleaseIntentClaim(ctx context.Context, id string, claimedBy string, maxRetries int) (int64, error)
	RecoverStaleIntents(ctx context.Context, cutoff time.Time, maxRetries int) (int64, error)
	StartIngestion(ctx context.Context, document *domain.Document, intentID string, claimedBy string) error
}

// Blob is what the hash worker needs from object storage.
type Blob interface {
	ExistsInUploadBucket(ctx context.Context, objectName string) (bool, error)
	HashUploadObject(ctx context.Context, objectName string) (string, int64, error)
	MigrateToIngestion(ctx context.Context, objectName string, hash string) (string, error)
}

// Config tunes the hash worker and recovery sweeper.
// JobTimeout must stay below StaleThreshold, so a stuck job gives up its claim
// before the recovery sweep hands it to another worker.
type Config struct {
	Interval         time.Duration // how often a worker looks for UPLOADED intents
	BatchSize        int           // intents fetched per pass
	JobTimeout       time.Duration // limit for hashing + moving + recording one intent
	MaxRetries       int           // attempts before an intent is marked FAILED
	RecoveryInterval time.Duration // how often the recovery sweep runs
	StaleThreshold   time.Duration // a HASHING claim older than this is presumed dead
}

func DefaultConfig() Config {
	return Config{
		Interval:         2 * time.Second,
		BatchSize:        20,
		JobTimeout:       2 * time.Minute,
		MaxRetries:       3,
		RecoveryInterval: 30 * time.Second,
		StaleThreshold:   5 * time.Minute,
	}
}

var (
	// ErrLostClaim means another worker owns the intent now; the caller must not touch it.
	ErrLostClaim = domain.ErrLostClaim
	// ErrNoTempObject means the uploaded object is gone; retrying cannot help.
	ErrNoTempObject = errors.New("missing temp object")
)

// NewWorkerID returns a claim owner ID unique per goroutine: "<hostname>/<uuid>".
func NewWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + "/hash/" + uuid.NewString()
}

// Worker claims UPLOADED intents, hashes and moves their objects, and records the result.
type Worker struct {
	id      string
	cfg     Config
	repo    Store
	storage Blob
	logger  *slog.Logger
}

func NewWorker(id string, cfg Config, repo Store, storage Blob, logger *slog.Logger) *Worker {
	return &Worker{id: id, cfg: cfg, repo: repo, storage: storage, logger: logger.With("worker", id)}
}

// Run polls for work every cfg.Interval until ctx is cancelled.
// Each worker owns its ticker: a shared ticker would deliver each tick to only one worker.
func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.pass(ctx)
		}
	}
}

func (w *Worker) pass(ctx context.Context) {
	intents, err := w.repo.GetIntentsForHashing(ctx, w.cfg.BatchSize)
	if err != nil {
		w.logger.Error("fetch intents for hashing failed", "err", err)
		return
	}
	for _, intent := range intents {
		if ctx.Err() != nil {
			return
		}
		now := time.Now().UTC()
		n, err := w.repo.UpdateIntentToHashing(ctx, intent.ID, &w.id, &now)
		if err != nil {
			w.logger.Error("claim intent failed", "intent", intent.ID, "err", err)
			continue
		}
		if n == 0 {
			continue // another worker claimed it first
		}
		w.process(ctx, intent)
	}
}

// process runs one claimed intent to an outcome. Every write after the claim is
// fenced by this worker's ID, so a worker that lost its claim cannot change the row.
func (w *Worker) process(ctx context.Context, intent *domain.Intent) {
	jobCtx, cancel := context.WithTimeout(ctx, w.cfg.JobTimeout)
	defer cancel()

	err := w.hashAndRecord(jobCtx, intent)
	switch {
	case err == nil:
		return
	case errors.Is(err, ErrLostClaim):
		w.logger.Info("lost claim; another worker owns the intent", "intent", intent.ID)
	case errors.Is(err, ErrNoTempObject):
		w.logger.Warn("temp object missing; marking intent failed", "intent", intent.ID)
		w.markFailed(ctx, intent.ID)
	default:
		w.logger.Error("hashing attempt failed", "intent", intent.ID, "err", err)
		w.releaseClaim(ctx, intent.ID)
	}
}

func (w *Worker) hashAndRecord(ctx context.Context, intent *domain.Intent) error {
	if intent.TempBucketPath == nil {
		return ErrNoTempObject
	}
	tempPath := *intent.TempBucketPath

	exists, err := w.storage.ExistsInUploadBucket(ctx, tempPath)
	if err != nil {
		return fmt.Errorf("check temp object: %w", err)
	}
	if !exists {
		return ErrNoTempObject
	}

	hash, size, err := w.storage.HashUploadObject(ctx, tempPath)
	if err != nil {
		return fmt.Errorf("hash temp object: %w", err)
	}
	// Content-addressed destination: moving a duplicate rewrites identical bytes, and
	// the move also removes the temp object.
	permPath, err := w.storage.MigrateToIngestion(ctx, tempPath, hash)
	if err != nil {
		return fmt.Errorf("move to ingestion bucket: %w", err)
	}

	document := &domain.Document{
		Hash:           hash,
		PermBucketPath: permPath,
		FileName:       intent.FileName,
		Size:           size,
	}
	if err := w.repo.StartIngestion(ctx, document, intent.ID, w.id); err != nil {
		return err
	}
	w.logger.Info("intent hashed", "intent", intent.ID, "hash", hash, "size", size, "path", permPath)
	return nil
}

// markFailed and releaseClaim use the worker's ctx, not the job's: the job may have
// failed precisely because its timeout expired.
func (w *Worker) markFailed(ctx context.Context, id string) {
	n, err := w.repo.MarkIntentFailed(ctx, id, w.id)
	if err != nil {
		w.logger.Error("mark intent failed: write failed", "intent", id, "err", err)
		return
	}
	if n == 0 {
		w.logger.Info("lost claim before marking failed", "intent", id)
	}
}

func (w *Worker) releaseClaim(ctx context.Context, id string) {
	n, err := w.repo.ReleaseIntentClaim(ctx, id, w.id, w.cfg.MaxRetries)
	if err != nil {
		w.logger.Error("release intent claim failed", "intent", id, "err", err)
		return
	}
	if n == 0 {
		w.logger.Info("lost claim before release", "intent", id)
	}
}

// Sweeper demotes HASHING intents whose worker stopped making progress.
type Sweeper struct {
	cfg    Config
	repo   Store
	logger *slog.Logger
}

func NewSweeper(cfg Config, repo Store, logger *slog.Logger) *Sweeper {
	return &Sweeper{cfg: cfg, repo: repo, logger: logger.With("component", "hash-recovery")}
}

// Run sweeps every cfg.RecoveryInterval until ctx is cancelled.
func (s *Sweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.RecoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().UTC().Add(-s.cfg.StaleThreshold)
			n, err := s.repo.RecoverStaleIntents(ctx, cutoff, s.cfg.MaxRetries)
			if err != nil {
				s.logger.Error("recover stale intents failed", "err", err)
				continue
			}
			if n > 0 {
				s.logger.Warn("recovered stale hashing claims", "count", n)
			}
		}
	}
}
