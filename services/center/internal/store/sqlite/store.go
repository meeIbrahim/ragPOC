package sqlite

import "database/sql"

// Store implements upload.Repository over a shared sqlite handle.
type Store struct {
	*IntentStore
	*DocumentStore
}

func NewStore(db *sql.DB) *Store {
	return &Store{
		IntentStore:   NewIntentStore(db),
		DocumentStore: NewDocumentStore(db),
	}
}
