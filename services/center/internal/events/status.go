package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// StatusEventType is the event_type field of an ingest status stream entry.
const StatusEventType = "IngestionStatus"

// StatusSchemaVersion is the only status payload version this consumer understands.
const StatusSchemaVersion = 1

type IngestionStatus string

const (
	StatusIndexed IngestionStatus = "INDEXED"
	StatusFailed  IngestionStatus = "FAILED"
)

// FailureReason is why ingestion failed. Every value in PRD 4.2 must be listed here, or a
// legitimate failure from the Python service would be discarded as unknown.
type FailureReason string

const (
	ReasonUnsupportedType       FailureReason = "UNSUPPORTED_TYPE"
	ReasonEmptyText             FailureReason = "EMPTY_TEXT"
	ReasonCorruptFile           FailureReason = "CORRUPT_FILE"
	ReasonDependencyUnavailable FailureReason = "DEPENDENCY_UNAVAILABLE"
	ReasonJobTimeout            FailureReason = "JOB_TIMEOUT"
	ReasonPoisonMessage         FailureReason = "POISON_MESSAGE"
	ReasonInternal              FailureReason = "INTERNAL"
)

var knownReasons = map[FailureReason]struct{}{
	ReasonUnsupportedType:       {},
	ReasonEmptyText:             {},
	ReasonCorruptFile:           {},
	ReasonDependencyUnavailable: {},
	ReasonJobTimeout:            {},
	ReasonPoisonMessage:         {},
	ReasonInternal:              {},
}

// ErrInvalidStatusEvent marks a payload the consumer must discard and acknowledge: retrying
// the same bytes can never succeed.
var ErrInvalidStatusEvent = errors.New("invalid status event")

// StatusEvent is the Python RAG service's report on one ingestion attempt (PRD 4.2).
type StatusEvent struct {
	// ID is for tracing only.
	ID     string
	Hash   string
	Status IngestionStatus
	// Attempts is the attempt number echoed from the ingest event. It is the CAS key against
	// document_store.retries, never a count Python keeps.
	Attempts int
	// Retryable and Reason are set on FAILED. Retryable is a pointer so an absent value is
	// distinguishable from false.
	Retryable *bool
	Reason    FailureReason
	// Detail is short free text for logs; it never carries document content.
	Detail     string
	ChunkCount *int
	CreatedAt  time.Time
}

// statusWire is the JSON shape. Fields whose absence must be detected are pointers.
type statusWire struct {
	ID            string          `json:"id"`
	Hash          string          `json:"hash"`
	Status        IngestionStatus `json:"status"`
	Attempts      *int            `json:"attempts"`
	Retryable     *bool           `json:"retryable"`
	Reason        FailureReason   `json:"reason"`
	Detail        string          `json:"detail"`
	ChunkCount    *int            `json:"chunk_count"`
	CreatedAt     time.Time       `json:"created_at"`
	SchemaVersion int             `json:"schema_version"`
}

// DecodeStatus parses and validates one status payload. Unknown JSON fields are ignored so a
// newer producer does not break this consumer; unknown schema versions, statuses and reasons
// are rejected. Every error wraps ErrInvalidStatusEvent.
func DecodeStatus(payload []byte) (StatusEvent, error) {
	var w statusWire
	if err := json.Unmarshal(payload, &w); err != nil {
		return StatusEvent{}, invalid("malformed json: %v", err)
	}
	if w.SchemaVersion != StatusSchemaVersion {
		return StatusEvent{}, invalid("unsupported schema_version %d", w.SchemaVersion)
	}
	if w.Hash == "" {
		return StatusEvent{}, invalid("hash is required")
	}
	if w.Attempts == nil || *w.Attempts < 0 {
		return StatusEvent{}, invalid("attempts must be present and >= 0")
	}
	if w.ChunkCount != nil && *w.ChunkCount < 0 {
		return StatusEvent{}, invalid("chunk_count must be >= 0")
	}
	if w.Reason != "" {
		if _, ok := knownReasons[w.Reason]; !ok {
			return StatusEvent{}, invalid("unknown reason %q", w.Reason)
		}
	}
	switch w.Status {
	case StatusIndexed:
	case StatusFailed:
		if w.Retryable == nil {
			return StatusEvent{}, invalid("retryable is required when status is FAILED")
		}
		if w.Reason == "" {
			return StatusEvent{}, invalid("reason is required when status is FAILED")
		}
	default:
		return StatusEvent{}, invalid("unknown status %q", w.Status)
	}

	return StatusEvent{
		ID:         w.ID,
		Hash:       w.Hash,
		Status:     w.Status,
		Attempts:   *w.Attempts,
		Retryable:  w.Retryable,
		Reason:     w.Reason,
		Detail:     w.Detail,
		ChunkCount: w.ChunkCount,
		CreatedAt:  w.CreatedAt,
	}, nil
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidStatusEvent, fmt.Sprintf(format, args...))
}
