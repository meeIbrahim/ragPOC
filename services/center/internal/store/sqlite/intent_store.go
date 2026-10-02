package sqlite

import (
	"center-service/internal/upload"
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

func (s *IntentStore) CreateIntent(ctx context.Context, i *upload.Intent) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO upload_intent (uuid, file_name, temp_bucket_path, hash, created_at, expires_at, uploaded_at, status) VALUES (?,?,?,?,?,?,?,?)`,
		i.ID, i.FileName, i.TempBucketPath, i.Hash, i.CreatedAt, i.ExpiresAt, nil, upload.StateWaiting.String())
	if err != nil {
		return fmt.Errorf("insert upload_intent: %w", err)
	}
	return nil
}

func (s *IntentStore) GetIntent(ctx context.Context, id string) (*upload.Intent, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT uuid, file_name, temp_bucket_path, hash, created_at, expires_at, uploaded_at, status FROM upload_intent WHERE uuid = ?`,
		id)

	var i upload.Intent
	var status string
	if err := row.Scan(&i.ID, &i.FileName, &i.TempBucketPath, &i.Hash, &i.CreatedAt, &i.ExpiresAt, &i.UploadedAt, &status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("upload_intent %s: %w", id, upload.ErrIntentNotFound)
		}
		return nil, fmt.Errorf("select upload_intent: %w", err)
	}
	i.Status = upload.StateFromString(status)
	return &i, nil
}

func (s *IntentStore) UpdateIntentToUploaded(ctx context.Context, id string, uploadedAt *time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE upload_intent SET status = ?, uploaded_at = ? WHERE uuid = ? AND status = ? AND expires_at > ?`,
		upload.StateUploaded.String(), uploadedAt, id, upload.StateWaiting.String(), uploadedAt)
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
		upload.StateExpired.String(), id, upload.StateWaiting.String(), now)
	if err != nil {
		return 0, fmt.Errorf("update upload_intent: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("update upload_intent: %w", err)
	}
	return n, nil
}

func (s *IntentStore) UpdateIntentToHashed(ctx context.Context, id string, hash *string, claimedBy *string) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE upload_intent SET status = ?, hash = ? WHERE uuid = ? AND status = ? AND claimed_by = ?`,
		upload.StateHashed.String(), hash, id, upload.StateHashing.String(), claimedBy)
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
		upload.StateHashing.String(), claimedBy, claimedAt, id, upload.StateUploaded.String())
	if err != nil {
		return 0, fmt.Errorf("update upload_intent: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("update upload_intent: %w", err)
	}
	return n, nil
}
