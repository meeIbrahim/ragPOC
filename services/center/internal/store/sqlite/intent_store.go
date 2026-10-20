package sqlite

import (
	"center-service/internal/domain"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type IntentStore struct {
	db *sql.DB
}

func NewIntentStore(db *sql.DB) *IntentStore {
	return &IntentStore{db: db}
}

func (s *IntentStore) CreateIntent(ctx context.Context, i *domain.Intent) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO upload_intent (uuid, file_name, temp_bucket_path, hash, created_at, expires_at, uploaded_at, status) VALUES (?,?,?,?,?,?,?,?)`,
		i.ID, i.FileName, i.TempBucketPath, i.Hash, i.CreatedAt, i.ExpiresAt, nil, domain.IntentWaiting.String())
	if err != nil {
		return fmt.Errorf("insert upload_intent: %w", err)
	}
	return nil
}

func (s *IntentStore) GetIntent(ctx context.Context, id string) (*domain.Intent, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT uuid, file_name, temp_bucket_path, hash, created_at, expires_at, uploaded_at, status FROM upload_intent WHERE uuid = ?`,
		id)

	var i domain.Intent
	var status string
	if err := row.Scan(&i.ID, &i.FileName, &i.TempBucketPath, &i.Hash, &i.CreatedAt, &i.ExpiresAt, &i.UploadedAt, &status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("upload_intent %s: %w", id, domain.ErrIntentNotFound)
		}
		return nil, fmt.Errorf("select upload_intent: %w", err)
	}
	i.Status = domain.StateFromString(status)
	return &i, nil
}

func (s *IntentStore) UpdateIntentToUploaded(ctx context.Context, id string, uploadedAt *time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE upload_intent SET status = ?, uploaded_at = ? WHERE uuid = ? AND status = ? AND expires_at > ?`,
		domain.IntentUploaded.String(), uploadedAt, id, domain.IntentWaiting.String(), uploadedAt)
	if err != nil {
		return 0, fmt.Errorf("update upload_intent: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("update upload_intent: %w", err)
	}
	return n, nil
}

func (s *IntentStore) UpdateIntentToExpired(ctx context.Context, id string, now *time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE upload_intent SET status = ? WHERE uuid = ? AND status = ? AND expires_at <= ?`,
		domain.IntentExpired.String(), id, domain.IntentWaiting.String(), now)
	if err != nil {
		return 0, fmt.Errorf("update upload_intent: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("update upload_intent: %w", err)
	}
	return n, nil
}

func (s *IntentStore) UpdateIntentToHashing(ctx context.Context, id string, claimedBy *string, claimedAt *time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE upload_intent SET status = ?, claimed_by = ?, claimed_at = ? WHERE uuid = ? AND status = ?`,
		domain.IntentHashing.String(), claimedBy, claimedAt, id, domain.IntentUploaded.String())
	if err != nil {
		return 0, fmt.Errorf("update upload_intent: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("update upload_intent: %w", err)
	}
	return n, nil
}

// GetIntentsForHashing returns up to limit UPLOADED intents, oldest first.
func (s *IntentStore) GetIntentsForHashing(ctx context.Context, limit int) ([]*domain.Intent, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT uuid, file_name, temp_bucket_path, hash, created_at, expires_at, uploaded_at, status, claimed_by, claimed_at, retries FROM upload_intent WHERE status = ? ORDER BY created_at ASC LIMIT ?`,
		domain.IntentUploaded.String(), limit)
	if err != nil {
		return nil, fmt.Errorf("select upload_intent: %w", err)
	}
	defer rows.Close()
	var intents []*domain.Intent
	for rows.Next() {
		var i domain.Intent
		var status string
		if err := rows.Scan(&i.ID, &i.FileName, &i.TempBucketPath, &i.Hash, &i.CreatedAt, &i.ExpiresAt, &i.UploadedAt, &status, &i.ClaimedBy, &i.ClaimedAt, &i.Retries); err != nil {
			return nil, fmt.Errorf("select upload_intent: %w", err)
		}
		i.Status = domain.StateFromString(status)
		intents = append(intents, &i)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("select upload_intent: %w", err)
	}
	return intents, nil
}

// MarkIntentFailed moves an intent this worker still owns straight to FAILED.
// 0 rows means the claim was lost and another worker owns the intent now.
func (s *IntentStore) MarkIntentFailed(ctx context.Context, id string, claimedBy string) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE upload_intent SET status = ?, claimed_by = NULL, claimed_at = NULL
		WHERE uuid = ? AND status = ? AND claimed_by = ?`,
		domain.IntentFailed.String(), id, domain.IntentHashing.String(), claimedBy)
	if err != nil {
		return 0, fmt.Errorf("mark upload_intent failed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("mark upload_intent failed: %w", err)
	}
	return n, nil
}

// ReleaseIntentClaim gives up this worker's claim after a failed attempt: back to
// UPLOADED for another try, or FAILED once retries reach maxRetries.
// 0 rows means the claim was lost and another worker owns the intent now.
func (s *IntentStore) ReleaseIntentClaim(ctx context.Context, id string, claimedBy string, maxRetries int) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE upload_intent
		SET status = CASE WHEN retries + 1 >= ? THEN ? ELSE ? END,
		    retries = retries + 1, claimed_by = NULL, claimed_at = NULL
		WHERE uuid = ? AND status = ? AND claimed_by = ?`,
		maxRetries, domain.IntentFailed.String(), domain.IntentUploaded.String(),
		id, domain.IntentHashing.String(), claimedBy)
	if err != nil {
		return 0, fmt.Errorf("release upload_intent claim: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("release upload_intent claim: %w", err)
	}
	return n, nil
}

// RecoverStaleIntents demotes HASHING intents whose claim is older than cutoff,
// counting it as a failed attempt. It is one statement, so it needs no per-row claim.
func (s *IntentStore) RecoverStaleIntents(ctx context.Context, cutoff time.Time, maxRetries int) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE upload_intent
		SET status = CASE WHEN retries + 1 >= ? THEN ? ELSE ? END,
		    retries = retries + 1, claimed_by = NULL, claimed_at = NULL
		WHERE status = ? AND claimed_at < ?`,
		maxRetries, domain.IntentFailed.String(), domain.IntentUploaded.String(),
		domain.IntentHashing.String(), cutoff)
	if err != nil {
		return 0, fmt.Errorf("recover stale upload_intent claims: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("recover stale upload_intent claims: %w", err)
	}
	return n, nil
}
