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
		`INSERT INTO upload_intents (id, file_name, object_path, created_at, expires_at, completed_at, status) VALUES (?,?,?,?,?,?,?)`,
		i.ID, i.FileName, i.ObjectPath, i.CreatedAt, i.ExpiresAt, i.CompletedAt, i.Status.String())
	if err != nil {
		return fmt.Errorf("insert upload_intent: %w", err)
	}
	return nil
}

func (s *IntentStore) GetIntent(ctx context.Context, id string) (*upload.Intent, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, file_name, object_path, created_at, expires_at, completed_at, status FROM upload_intents WHERE id = ?`,
		id)

	var i upload.Intent
	var status string
	if err := row.Scan(&i.ID, &i.FileName, &i.ObjectPath, &i.CreatedAt, &i.ExpiresAt, &i.CompletedAt, &status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("upload_intent %s: %w", id, sql.ErrNoRows)
		}
		return nil, fmt.Errorf("select upload_intent: %w", err)
	}
	i.Status = stateFromString(status)
	return &i, nil
}

func (s *IntentStore) UpdateIntentStatus(ctx context.Context, id string, status upload.State, completedAt *time.Time) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE upload_intents SET status = ?, completed_at = ? WHERE id = ?`,
		status.String(), completedAt, id)
	if err != nil {
		return fmt.Errorf("update upload_intent: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update upload_intent: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("upload_intent %s: %w", id, sql.ErrNoRows)
	}
	return nil
}

func stateFromString(s string) upload.State {
	for state, name := range map[upload.State]string{
		upload.StateUnknown:            "unknown",
		upload.StateWaiting:            "waiting",
		upload.StateUploaded:           "uploaded",
		upload.StateProcessing:         "processing",
		upload.StateCompleted:          "completed",
		upload.StateDuplicate:          "duplicate",
		upload.StateFailed:             "failed",
		upload.StateExpired:            "expired",
		upload.StateVerificationFailed: "verification_failed",
	} {
		if name == s {
			return state
		}
	}
	return upload.StateUnknown
}
