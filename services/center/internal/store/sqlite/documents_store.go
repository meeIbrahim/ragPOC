package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"center-service/internal/domain"
)

// listDocumentsQuery reads the whole list in one statement, so a concurrent hash cannot show an
// upload both as pending and as merged.
//
// entries holds one row per list entry: per hash, the oldest linked upload (ROW_NUMBER keeps
// created_at a plain column, which modernc needs to scan a time), plus every upload without a
// document that the UI should see. The join back to upload_intent then gives one row per
// linked upload id, in entry order; ListDocuments folds them into entries. upload_intent.uuid
// breaks every tie, so the order is the same on every poll.
const listDocumentsQuery = `
WITH entries AS (
	SELECT id, hash, file_name, status, failure_reason, created_at FROM (
		SELECT ui.uuid AS id, ui.hash AS hash, ds.file_name AS file_name, ds.status AS status,
			ds.failure_reason AS failure_reason, ui.created_at AS created_at,
			ROW_NUMBER() OVER (PARTITION BY ui.hash ORDER BY ui.created_at, ui.uuid) AS rn
		FROM upload_intent ui
		JOIN document_store ds ON ds.hash = ui.hash
	) WHERE rn = 1
	UNION ALL
	SELECT uuid, NULL, file_name,
		CASE status WHEN 'FAILED' THEN 'FAILED' ELSE 'PROCESSING' END,
		CASE status WHEN 'FAILED' THEN ? END,
		created_at
	FROM upload_intent
	WHERE hash IS NULL AND status IN ('UPLOADED', 'HASHING', 'HASHED', 'FAILED')
	ORDER BY created_at DESC, id DESC
	LIMIT ?
)
SELECT e.id, e.file_name, e.status, e.failure_reason, e.created_at, ui.uuid
FROM entries e
JOIN upload_intent ui ON ui.uuid = e.id OR ui.hash = e.hash
ORDER BY e.created_at DESC, e.id DESC, ui.created_at, ui.uuid`

// ListDocuments returns up to limit entries, newest first, and whether more exist. An upload
// still WAITING or EXPIRED is not listed; one in UPLOADED or HASHING (or HASHED without a
// hash, which the poll also reports as PROCESSING) is its own PROCESSING entry; one that
// failed in hashing is FAILED with domain.ReasonUploadFailed.
func (s *Store) ListDocuments(ctx context.Context, limit int) ([]domain.DocumentEntry, bool, error) {
	rows, err := s.db.QueryContext(ctx, listDocumentsQuery, domain.ReasonUploadFailed, limit+1)
	if err != nil {
		return nil, false, fmt.Errorf("list documents: %w", err)
	}
	defer rows.Close()

	entries := []domain.DocumentEntry{}
	for rows.Next() {
		var (
			e        domain.DocumentEntry
			reason   sql.NullString
			uploadID string
		)
		if err := rows.Scan(&e.ID, &e.FileName, &e.Status, &reason, &e.CreatedAt, &uploadID); err != nil {
			return nil, false, fmt.Errorf("list documents: %w", err)
		}
		if n := len(entries); n > 0 && entries[n-1].ID == e.ID {
			entries[n-1].UploadIDs = append(entries[n-1].UploadIDs, uploadID)
			continue
		}
		if e.Status == domain.DocumentFailed {
			e.FailureReason = reason.String
		}
		e.UploadIDs = []string{uploadID}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("list documents: %w", err)
	}
	if len(entries) > limit {
		return entries[:limit], true, nil
	}
	return entries, false, nil
}
