// Package status consumes the ingest status stream and applies each event to document_store.
package status

import (
	"context"
	"log/slog"
	"time"

	"center-service/internal/domain"
	"center-service/internal/events"
)

// Store is what applying a status event needs from persistence.
type Store interface {
	ApplyIndexed(ctx context.Context, hash string, attempts int, now time.Time) (applied bool, err error)
	ApplyFailed(ctx context.Context, hash string, attempts int, retryable bool, maxAttempts int, now time.Time) (domain.FailedOutcome, error)
}

// maxLoggedDetail caps the producer's free-text detail so a misbehaving producer cannot flood
// the logs.
const maxLoggedDetail = 256

// Handler turns one status stream entry into a store call and says whether to acknowledge it.
type Handler struct {
	store        Store
	maxAttempts  int
	entryTimeout time.Duration
	now          func() time.Time
	logger       *slog.Logger
}

// NewHandler builds a Handler. maxAttempts is [ingestion].max_attempts and entryTimeout is
// [go.status_consumer].entry_timeout.
func NewHandler(store Store, maxAttempts int, entryTimeout time.Duration, logger *slog.Logger) *Handler {
	return &Handler{
		store:        store,
		maxAttempts:  maxAttempts,
		entryTimeout: entryTimeout,
		now:          func() time.Time { return time.Now().UTC() },
		logger:       logger.With("component", "status-handler"),
	}
}

// Handle applies one entry and reports whether the caller should XACK it.
//
// It returns true for every entry that is finished, including malformed ones and stale ones:
// retrying the same bytes can never succeed, and leaving them pending would redeliver them
// forever. It returns false only when the database call failed, so the entry is retried.
func (h *Handler) Handle(ctx context.Context, entry events.StreamEntry) (ack bool) {
	log := h.logger.With("entry", entry.ID)

	if eventType, _ := entry.Values["event_type"].(string); eventType != events.StatusEventType {
		log.Warn("discarding entry with unexpected event_type", "event_type", entry.Values["event_type"])
		return true
	}
	payload, ok := payloadBytes(entry.Values["payload"])
	if !ok {
		log.Warn("discarding entry without a text payload")
		return true
	}
	ev, err := events.DecodeStatus(payload)
	if err != nil {
		log.Warn("discarding invalid status event", "err", err)
		return true
	}
	log = log.With("hash", ev.Hash, "attempts", ev.Attempts, "status", ev.Status)

	ctx, cancel := context.WithTimeout(ctx, h.entryTimeout)
	defer cancel()

	switch ev.Status {
	case events.OutcomeIndexed:
		applied, err := h.store.ApplyIndexed(ctx, ev.Hash, ev.Attempts, h.now())
		if err != nil {
			log.Error("apply indexed failed; leaving the entry pending", "err", err)
			return false
		}
		if !applied {
			log.Info("stale or duplicate INDEXED ignored")
			return true
		}
		log.Info("document indexed")
	case events.OutcomeFailed:
		retryable := *ev.Retryable // DecodeStatus guarantees it is set on FAILED
		log = log.With("reason", ev.Reason, "retryable", retryable, "detail", clip(ev.Detail))
		outcome, err := h.store.ApplyFailed(ctx, ev.Hash, ev.Attempts, retryable, h.maxAttempts, h.now())
		if err != nil {
			log.Error("apply failed errored; leaving the entry pending", "err", err)
			return false
		}
		switch outcome {
		case domain.FailedStale:
			log.Info("stale or duplicate FAILED ignored")
		case domain.FailedRetried:
			log.Info("ingestion failed; next attempt queued", "next_attempt", ev.Attempts+1)
		case domain.FailedExhausted:
			log.Warn("ingestion failed; document marked FAILED")
		}
	}
	return true
}

func payloadBytes(v any) ([]byte, bool) {
	switch p := v.(type) {
	case string:
		return []byte(p), true
	case []byte:
		return p, true
	}
	return nil, false
}

func clip(s string) string {
	if len(s) <= maxLoggedDetail {
		return s
	}
	return s[:maxLoggedDetail] + "..."
}
