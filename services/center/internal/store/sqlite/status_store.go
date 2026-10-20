package sqlite

import (
	"context"
	"fmt"
	"time"

	"center-service/internal/domain"
)

// ApplyIndexed records that attempt `attempts` of the document finished successfully.
//
// It is a compare-and-swap on (hash, retries) that only touches a PROCESSING document, so a
// redelivered event, a superseded attempt, and a late INDEXED after the document already
// failed are all no-ops. applied is false for those cases; it is not an error.
func (s *Store) ApplyIndexed(ctx context.Context, hash string, attempts int, now time.Time) (applied bool, err error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE document_store SET status = ?, updated_at = ?
		WHERE hash = ? AND retries = ? AND status = ?`,
		domain.DocumentIndexed, now, hash, attempts, domain.DocumentProcessing)
	if err != nil {
		return false, fmt.Errorf("apply indexed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("apply indexed: %w", err)
	}
	return n == 1, nil
}

// ApplyFailed records that attempt `attempts` of the document failed.
//
// The compare-and-swap guard is the same as ApplyIndexed. When the failure is retryable and
// attempts+1 is still below maxAttempts, the retries counter moves to attempts+1 and one
// outbox row is inserted in the same transaction, so the document never claims a retry that
// no event will carry. Otherwise the document becomes FAILED and nothing is queued.
// retries only grows; it is never reset.
func (s *Store) ApplyFailed(ctx context.Context, hash string, attempts int, retryable bool, maxAttempts int, now time.Time) (domain.FailedOutcome, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.FailedStale, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	retry := retryable && attempts+1 < maxAttempts

	var update string
	var args []any
	if retry {
		update = `UPDATE document_store SET retries = ?, updated_at = ?
			WHERE hash = ? AND retries = ? AND status = ?`
		args = []any{attempts + 1, now, hash, attempts, domain.DocumentProcessing}
	} else {
		update = `UPDATE document_store SET status = ?, updated_at = ?
			WHERE hash = ? AND retries = ? AND status = ?`
		args = []any{domain.DocumentFailed, now, hash, attempts, domain.DocumentProcessing}
	}
	res, err := tx.ExecContext(ctx, update, args...)
	if err != nil {
		return domain.FailedStale, fmt.Errorf("apply failed: update document_store: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return domain.FailedStale, fmt.Errorf("apply failed: update document_store: %w", err)
	}
	if n == 0 {
		return domain.FailedStale, nil
	}

	if retry {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO outbox (hash, status, created_at) VALUES (?,?,?)`,
			hash, domain.OutboxPending, now); err != nil {
			return domain.FailedStale, fmt.Errorf("apply failed: insert outbox: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return domain.FailedStale, fmt.Errorf("apply failed: commit: %w", err)
	}
	if retry {
		return domain.FailedRetried, nil
	}
	return domain.FailedExhausted, nil
}
