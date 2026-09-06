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
		`SELECT content_hash, object_path, file_name, created_at, size FROM documents WHERE content_hash = ?`,
		hash)

	var d upload.DocumentStorage
	if err := row.Scan(&d.ContentHash, &d.ObjectPath, &d.FileName, &d.CreatedAt, &d.Size); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("select document: %w", err)
	}
	return &d, nil
}

func (s *DocumentStore) CreateDocument(ctx context.Context, d *upload.DocumentStorage) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO documents (content_hash, object_path, file_name, created_at, size) VALUES (?,?,?,?,?)`,
		d.ContentHash, d.ObjectPath, d.FileName, d.CreatedAt, d.Size)
	if err != nil {
		return fmt.Errorf("insert document: %w", err)
	}
	return nil
}
