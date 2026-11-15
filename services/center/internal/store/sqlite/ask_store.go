package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"center-service/internal/domain"
)

// ResolveUploads maps upload ids to their content hash and document status in one query.
// An id with no upload_intent row is absent from the result, so the caller can report it as
// unknown. Hash is empty while an upload is not hashed yet; DocStatus is empty when no
// document row exists. Duplicate ids yield one ref.
func (s *Store) ResolveUploads(ctx context.Context, ids []string) ([]domain.UploadRef, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT ui.uuid, ui.hash, ds.status
		FROM upload_intent ui
		LEFT JOIN document_store ds ON ds.hash = ui.hash
		WHERE ui.uuid IN (?`+strings.Repeat(",?", len(ids)-1)+`)`,
		args...)
	if err != nil {
		return nil, fmt.Errorf("resolve uploads: %w", err)
	}
	defer rows.Close()

	var refs []domain.UploadRef
	for rows.Next() {
		var (
			ref    domain.UploadRef
			hash   sql.NullString
			status sql.NullString
		)
		if err := rows.Scan(&ref.UploadID, &hash, &status); err != nil {
			return nil, fmt.Errorf("resolve uploads: %w", err)
		}
		ref.Hash = hash.String
		ref.DocStatus = domain.DocumentStatus(status.String)
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("resolve uploads: %w", err)
	}
	return refs, nil
}
