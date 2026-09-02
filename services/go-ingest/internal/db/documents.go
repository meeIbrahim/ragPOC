package db

import (
	"database/sql"
	"time"
)

type Document struct {
	HashID      string
	ObjectPath  string
	Status      string
	PublishedAt sql.NullTime
	ConfirmedAt time.Time
}

func GetByHash(conn *sql.DB, hashID string) (*Document, error) {
	row := conn.QueryRow(
		`SELECT hash_id, object_path, status, published_at, confirmed_at FROM documents WHERE hash_id = ?`,
		hashID,
	)
	var d Document
	if err := row.Scan(&d.HashID, &d.ObjectPath, &d.Status, &d.PublishedAt, &d.ConfirmedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &d, nil
}

func Insert(conn *sql.DB, hashID, objectPath string) error {
	_, err := conn.Exec(
		`INSERT INTO documents (hash_id, object_path, status, confirmed_at) VALUES (?, ?, 'uploaded', ?)`,
		hashID, objectPath, time.Now().UTC(),
	)
	return err
}

// InsertIfAbsent inserts a new document row unless hash_id already exists.
// Returns true if the row was newly inserted, false if it already existed.
func InsertIfAbsent(conn *sql.DB, hashID, objectPath string) (bool, error) {
	result, err := conn.Exec(
		`INSERT INTO documents (hash_id, object_path, status, confirmed_at)
		 VALUES (?, ?, 'uploaded', ?)
		 ON CONFLICT(hash_id) DO NOTHING`,
		hashID, objectPath, time.Now().UTC(),
	)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func MarkPublished(conn *sql.DB, hashID string) error {
	_, err := conn.Exec(`UPDATE documents SET published_at = ? WHERE hash_id = ?`, time.Now().UTC(), hashID)
	return err
}

func UnpublishedHashes(conn *sql.DB) ([]Document, error) {
	rows, err := conn.Query(
		`SELECT hash_id, object_path, status, published_at, confirmed_at FROM documents WHERE published_at IS NULL`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var docs []Document
	for rows.Next() {
		var d Document
		if err := rows.Scan(&d.HashID, &d.ObjectPath, &d.Status, &d.PublishedAt, &d.ConfirmedAt); err != nil {
			return nil, err
		}
		docs = append(docs, d)
	}
	return docs, rows.Err()
}
