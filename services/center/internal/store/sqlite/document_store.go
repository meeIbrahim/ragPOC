package sqlite

import (
	"center-service/internal/domain"
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type DocumentStore struct {
	db *sql.DB
}

func NewDocumentStore(db *sql.DB) *DocumentStore {
	return &DocumentStore{db: db}
}

func (s *DocumentStore) GetDocumentByHash(ctx context.Context, hash string) (*domain.Document, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT hash, perm_bucket_path, file_name, size, status, retries, created_at, updated_at FROM document_store WHERE hash = ?`,
		hash)

	var d domain.Document
	if err := row.Scan(&d.Hash, &d.PermBucketPath, &d.FileName, &d.Size, &d.Status, &d.Retries, &d.CreatedAt, &d.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("select document: %w", err)
	}
	return &d, nil
}
