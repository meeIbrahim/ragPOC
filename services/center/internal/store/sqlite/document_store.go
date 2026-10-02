package sqlite

import (
	"center-service/internal/upload"
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

func (s *DocumentStore) GetDocumentByHash(ctx context.Context, hash string) (*upload.DocumentStorage, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT hash, perm_bucket_path, file_name, size, status, retries, created_at, updated_at FROM document_store WHERE hash = ?`,
		hash)

	var d upload.DocumentStorage
	if err := row.Scan(&d.Hash, &d.PermBucketPath, &d.FileName, &d.Size, &d.Status, &d.Retries, &d.CreatedAt, &d.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("select document: %w", err)
	}
	return &d, nil
}

func (s *DocumentStore) CreateDocument(ctx context.Context, d *upload.DocumentStorage) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO document_store (hash, perm_bucket_path, file_name, size, status, retries, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)`,
		d.Hash, d.PermBucketPath, d.FileName, d.Size, d.Status, d.Retries, d.CreatedAt, d.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert document: %w", err)
	}
	return nil
}
