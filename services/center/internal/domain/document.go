package domain

import "time"

type DocumentStatus string

const (
	DocumentProcessing DocumentStatus = "PROCESSING"
	DocumentIndexed    DocumentStatus = "INDEXED"
	DocumentFailed     DocumentStatus = "FAILED"
)

type Document struct {
	Hash           string
	PermBucketPath string
	FileName       string
	Size           int64
	Status         DocumentStatus
	Retries        int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
