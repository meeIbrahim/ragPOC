package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

// StatusEventType is the event_type field of an ingest status stream entry.
const StatusEventType = "IngestionStatus"

// StatusSchemaVersion is the only status payload version this consumer understands.
const StatusSchemaVersion = 1

// Outcome is how one ingestion attempt ended.
type Outcome string

const (
	OutcomeIndexed Outcome = "INDEXED"
	OutcomeFailed  Outcome = "FAILED"
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
	// ID is for tracing only. Go does not require it, so it may be empty.
	ID     string
	Hash   string
	Status Outcome
	// Attempts is the attempt number echoed from the ingest event. It is the CAS key against
	// document_store.retries, never a count Python keeps.
	Attempts int
	// Retryable and Reason are required on FAILED and ignored on INDEXED. Retryable is a
	// pointer so an absent value is distinguishable from false.
	Retryable *bool
	Reason    FailureReason
	// Detail is short free text for logs; it never carries document content.
	Detail     string
	ChunkCount *int
	// CreatedAt is for logs only. Go does not require it, so it may be the zero time.
	CreatedAt time.Time
}

// maxInteger bounds integer fields. It is the int32 range so the result is the same on every
// platform, far below 2^53 where a float64 stops holding integers exactly, and far above any
// real attempt number or chunk count. The JSON schema carries the same maximum.
const maxInteger = math.MaxInt32

// maxEchoed caps how much of a bad field value an error message repeats.
const maxEchoed = 64

// DecodeStatus parses and validates one status payload.
//
// Keys are matched exactly (encoding/json would match "Status" to "status"), the same as the
// JSON schema and Python's json module, so both sides read the same status from one payload.
// Integral numbers such as 1.0 or 1e0 are accepted for integer fields, as JSON Schema does.
// null counts as absent. created_at is only logged, so an unparsable one is treated as absent
// rather than discarding a valid status. Unknown fields are ignored so a newer producer does not break this
// consumer, but unknown schema versions, statuses and reasons are rejected.
// Every error wraps ErrInvalidStatusEvent.
func DecodeStatus(payload []byte) (StatusEvent, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return StatusEvent{}, invalid("malformed json: %v", err)
	}
	if fields == nil {
		return StatusEvent{}, invalid("payload must be a JSON object")
	}

	d := fieldReader{fields: fields}
	ev := StatusEvent{
		ID:         d.str("id"),
		Hash:       d.str("hash"),
		Status:     Outcome(d.str("status")),
		Reason:     FailureReason(d.str("reason")),
		Detail:     d.str("detail"),
		Retryable:  d.boolean("retryable"),
		ChunkCount: d.integer("chunk_count"),
		CreatedAt:  d.timestamp("created_at"),
	}
	attempts := d.integer("attempts")
	version := d.integer("schema_version")
	if d.err != nil {
		return StatusEvent{}, d.err
	}

	if version == nil || *version != StatusSchemaVersion {
		return StatusEvent{}, invalid("unsupported schema_version %v", describe(version))
	}
	if ev.Hash == "" {
		return StatusEvent{}, invalid("hash is required")
	}
	if ev.Status == "" {
		return StatusEvent{}, invalid("status is required")
	}
	if attempts == nil || *attempts < 0 {
		return StatusEvent{}, invalid("attempts must be present and >= 0")
	}
	ev.Attempts = *attempts
	if ev.ChunkCount != nil && *ev.ChunkCount < 0 {
		return StatusEvent{}, invalid("chunk_count must be >= 0")
	}
	if ev.Reason != "" {
		if _, ok := knownReasons[ev.Reason]; !ok {
			return StatusEvent{}, invalid("unknown reason %q", ev.Reason)
		}
	}
	switch ev.Status {
	case OutcomeIndexed:
	case OutcomeFailed:
		if ev.Retryable == nil {
			return StatusEvent{}, invalid("retryable is required when status is FAILED")
		}
		if ev.Reason == "" {
			return StatusEvent{}, invalid("reason is required when status is FAILED")
		}
	default:
		return StatusEvent{}, invalid("unknown status %q", ev.Status)
	}
	return ev, nil
}

// fieldReader reads typed values out of a decoded JSON object. A field that is absent or null
// yields the zero value; a present field of the wrong type records the first error.
type fieldReader struct {
	fields map[string]json.RawMessage
	err    error
}

func (r *fieldReader) raw(key string) (json.RawMessage, bool) {
	v, ok := r.fields[key]
	if !ok || string(v) == "null" {
		return nil, false
	}
	return v, true
}

func (r *fieldReader) fail(key string, err error) {
	if r.err == nil {
		r.err = invalid("field %q: %v", key, err)
	}
}

func (r *fieldReader) str(key string) string {
	v, ok := r.raw(key)
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(v, &s); err != nil {
		r.fail(key, err)
	}
	return s
}

func (r *fieldReader) boolean(key string) *bool {
	v, ok := r.raw(key)
	if !ok {
		return nil
	}
	var b bool
	if err := json.Unmarshal(v, &b); err != nil {
		r.fail(key, err)
		return nil
	}
	return &b
}

func (r *fieldReader) integer(key string) *int {
	v, ok := r.raw(key)
	if !ok {
		return nil
	}
	f, err := strconv.ParseFloat(string(v), 64)
	if err != nil {
		r.fail(key, fmt.Errorf("not a number: %s", clip(v)))
		return nil
	}
	if f != math.Trunc(f) || math.Abs(f) > maxInteger {
		r.fail(key, fmt.Errorf("not an integer in range: %s", clip(v)))
		return nil
	}
	n := int(f)
	return &n
}

func (r *fieldReader) timestamp(key string) time.Time {
	v, ok := r.raw(key)
	if !ok {
		return time.Time{}
	}
	var t time.Time
	if err := json.Unmarshal(v, &t); err != nil {
		return time.Time{}
	}
	return t
}

func clip(v json.RawMessage) string {
	if len(v) <= maxEchoed {
		return string(v)
	}
	return string(v[:maxEchoed]) + "..."
}

func describe(n *int) any {
	if n == nil {
		return "(missing)"
	}
	return *n
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidStatusEvent, fmt.Sprintf(format, args...))
}
