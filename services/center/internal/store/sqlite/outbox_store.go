package sqlite

import (
	"center-service/internal/domain"
	"context"
	"database/sql"
	"fmt"
	"time"
)

type OutboxStore struct {
	db *sql.DB
}

func NewOutboxStore(db *sql.DB) *OutboxStore {
	return &OutboxStore{db: db}
}

func (s *OutboxStore) FetchCandidates(ctx context.Context, limit int) ([]*domain.OutboxMessage, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, hash, status, claimed_by, claimed_at, retries FROM outbox WHERE status = ? ORDER BY created_at ASC LIMIT ?`,
		domain.OutboxPending, limit)
	if err != nil {
		return nil, fmt.Errorf("select outbox: %w", err)
	}
	defer rows.Close()
	var candidates []*domain.OutboxMessage
	for rows.Next() {
		var c domain.OutboxMessage
		if err := rows.Scan(&c.Id, &c.Hash, &c.Status, &c.ClaimedBy, &c.ClaimedAt, &c.Retries); err != nil {
			return nil, fmt.Errorf("select outbox: %w", err)
		}
		candidates = append(candidates, &c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("select outbox: %w", err)
	}
	return candidates, nil
}

func (s *OutboxStore) ClaimOutboxMessage(ctx context.Context, claimedBy string, id int64) (bool, error) {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx,
		`UPDATE outbox SET status = ?, claimed_by = ?, claimed_at = ? WHERE id = ? AND status = ?`,
		domain.OutboxClaimed, claimedBy, now, id, domain.OutboxPending)

	if err != nil {
		return false, fmt.Errorf("update outbox: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("update outbox: %w", err)
	}
	return n > 0, nil
}

func (s *OutboxStore) MarkPublished(ctx context.Context, id int64, claimedBy string) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE outbox set status = ?, claimed_by = NULL, claimed_at = NULL WHERE id = ? AND status = ? AND claimed_by = ?`,
		domain.OutboxPublished, id, domain.OutboxClaimed, claimedBy)
	if err != nil {
		return false, fmt.Errorf("update outbox: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("update outbox: %w", err)
	}
	return n > 0, nil
}

func (s *OutboxStore) RecoverStaleOutbox(ctx context.Context, cutoff time.Time, maxRetries int) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE outbox
		SET status = CASE WHEN retries + 1 >= ? THEN ? ELSE ? END,
		retries = retries + 1, claimed_by = NULL, claimed_at = NULL
		WHERE status = ? AND claimed_at < ?`,
		maxRetries, domain.OutboxBlocked, domain.OutboxPending, domain.OutboxClaimed, cutoff)
	if err != nil {
		return 0, fmt.Errorf("recover stale outbox claims: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("recover stale outbox claims: %w", err)
	}
	return n, nil
}

func (s *OutboxStore) ReleaseClaim(ctx context.Context, id int64, claimedBy string, maxRetries int) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE outbox
		SET status = CASE WHEN retries + 1 >= ? THEN ? ELSE ? END,
		retries = retries + 1, claimed_by = NULL, claimed_at = NULL
		WHERE status = ? AND id = ? AND claimed_by = ?`,
		maxRetries, domain.OutboxBlocked, domain.OutboxPending, domain.OutboxClaimed, id, claimedBy)
	if err != nil {
		return false, fmt.Errorf("release outbox claim: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("release outbox claim: %w", err)
	}
	return n > 0, nil
}

func (s *OutboxStore) MarkBlocked(ctx context.Context, id int64, claimedBy string) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE outbox
		SET status = ?, claimed_by = NULL, claimed_at = NULL
		WHERE status = ? AND id = ? AND claimed_by = ?`,
		domain.OutboxBlocked, domain.OutboxClaimed, id, claimedBy)
	if err != nil {
		return false, fmt.Errorf("mark outbox blocked: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("mark outbox blocked: %w", err)
	}
	return n > 0, nil
}
