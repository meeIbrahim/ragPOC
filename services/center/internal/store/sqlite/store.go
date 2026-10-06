package sqlite

import (
	"center-service/internal/domain"
	"center-service/internal/hashing"
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Store struct {
	*IntentStore
	*DocumentStore
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{
		IntentStore:   NewIntentStore(db),
		DocumentStore: NewDocumentStore(db),
		db:            db,
	}
}

// StartIngestion records a hashed upload in one transaction: the document row (if the
// hash is new) plus its outbox event, then the intent moves HASHING -> HASHED. The intent
// update is fenced by claimedBy; if this worker lost its claim, nothing is written and
// hashing.ErrLostClaim is returned.
func (s *Store) StartIngestion(ctx context.Context, document *domain.Document, intentID string, claimedBy string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()

	res, err := tx.ExecContext(ctx,
		`INSERT INTO document_store (hash, perm_bucket_path, file_name, size, status, retries, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(hash) DO NOTHING`,
		document.Hash, document.PermBucketPath, document.FileName, document.Size, domain.DocumentProcessing, 0, now, now)
	if err != nil {
		return fmt.Errorf("insert document_store: %w", err)
	}
	created, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("insert document_store: %w", err)
	}
	// Only the upload that created the document queues its ingestion event;
	// duplicates just link to the existing row.
	if created == 1 {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO outbox (hash, status, created_at) VALUES (?,?,?)`,
			document.Hash, domain.OutboxPending, now); err != nil {
			return fmt.Errorf("insert outbox: %w", err)
		}
	}

	res, err = tx.ExecContext(ctx,
		`UPDATE upload_intent SET status = ?, hash = ? WHERE uuid = ? AND status = ? AND claimed_by = ?`,
		domain.IntentHashed.String(), document.Hash, intentID, domain.IntentHashing.String(), claimedBy)
	if err != nil {
		return fmt.Errorf("update upload_intent to hashed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update upload_intent to hashed: %w", err)
	}
	if n == 0 {
		return hashing.ErrLostClaim
	}

	return tx.Commit()
}
