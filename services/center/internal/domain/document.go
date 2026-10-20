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

// FailedOutcome is what applying a FAILED status event did to the document.
type FailedOutcome int

const (
	// FailedStale: the event matched no PROCESSING document at that attempt (duplicate,
	// superseded attempt, or the document already finished). Nothing was written.
	FailedStale FailedOutcome = iota
	// FailedRetried: the next attempt was queued (retries bumped, one outbox row inserted).
	FailedRetried
	// FailedExhausted: the document was marked FAILED, either because the failure is
	// permanent or because no attempts are left.
	FailedExhausted
)

func (o FailedOutcome) String() string {
	switch o {
	case FailedStale:
		return "stale"
	case FailedRetried:
		return "retried"
	case FailedExhausted:
		return "exhausted"
	}
	return "unknown"
}
