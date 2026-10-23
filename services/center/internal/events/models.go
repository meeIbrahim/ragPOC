package events

import "time"

type StreamIngestionEvent struct {
	// ID is for tracing only; it is new on every publish. The Python consumer dedupes on Hash;
	// Go's status CAS key is (Hash, Retries).
	ID            string    `json:"id"`
	Hash          string    `json:"hash"`
	Filename      string    `json:"filename"`
	BucketPath    string    `json:"bucket_path"`
	Size          int64     `json:"size"`
	Attempts      int       `json:"attempts"`
	CreatedAt     time.Time `json:"created_at"`
	SchemaVersion int       `json:"schema_version"`
}
