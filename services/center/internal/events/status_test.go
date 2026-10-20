package events

import (
	"errors"
	"strings"
	"testing"
)

const testHash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"

func payload(fields string) []byte {
	return []byte(`{"id":"9b2c0f1e-0000-4000-8000-000000000001","hash":"` + testHash + `",` +
		`"created_at":"2026-10-14T10:00:00.123456Z","schema_version":1,` + fields + `}`)
}

func TestDecodeStatusIndexed(t *testing.T) {
	ev, err := DecodeStatus(payload(`"status":"INDEXED","attempts":2,"chunk_count":12`))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Status != StatusIndexed || ev.Hash != testHash || ev.Attempts != 2 {
		t.Errorf("event = %+v", ev)
	}
	if ev.ChunkCount == nil || *ev.ChunkCount != 12 {
		t.Errorf("ChunkCount = %v", ev.ChunkCount)
	}
	if ev.CreatedAt.IsZero() {
		t.Error("created_at not parsed")
	}
}

func TestDecodeStatusIndexedWithoutOptionals(t *testing.T) {
	ev, err := DecodeStatus(payload(`"status":"INDEXED","attempts":0`))
	if err != nil {
		t.Fatal(err)
	}
	if ev.ChunkCount != nil || ev.Retryable != nil || ev.Reason != "" {
		t.Errorf("optionals should be unset: %+v", ev)
	}
}

func TestDecodeStatusFailed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fields    string
		retryable bool
		reason    FailureReason
	}{
		{"retryable", `"status":"FAILED","attempts":1,"retryable":true,"reason":"DEPENDENCY_UNAVAILABLE"`, true, ReasonDependencyUnavailable},
		{"permanent", `"status":"FAILED","attempts":0,"retryable":false,"reason":"UNSUPPORTED_TYPE"`, false, ReasonUnsupportedType},
		{"poison", `"status":"FAILED","attempts":2,"retryable":false,"reason":"POISON_MESSAGE"`, false, ReasonPoisonMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev, err := DecodeStatus(payload(tc.fields))
			if err != nil {
				t.Fatal(err)
			}
			if ev.Status != StatusFailed || ev.Retryable == nil || *ev.Retryable != tc.retryable || ev.Reason != tc.reason {
				t.Errorf("event = %+v", ev)
			}
		})
	}
}

// Every reason in PRD 4.2 must be known to Go, or a legitimate failure would be discarded.
func TestDecodeStatusAcceptsEveryPRDReason(t *testing.T) {
	for _, reason := range []string{
		"UNSUPPORTED_TYPE", "EMPTY_TEXT", "CORRUPT_FILE", "DEPENDENCY_UNAVAILABLE",
		"JOB_TIMEOUT", "POISON_MESSAGE", "INTERNAL",
	} {
		if _, err := DecodeStatus(payload(`"status":"FAILED","attempts":0,"retryable":false,"reason":"` + reason + `"`)); err != nil {
			t.Errorf("reason %s rejected: %v", reason, err)
		}
	}
}

// New fields from a newer Python must not break an older Go consumer.
func TestDecodeStatusIgnoresUnknownFields(t *testing.T) {
	if _, err := DecodeStatus(payload(`"status":"INDEXED","attempts":0,"future_field":{"a":1}`)); err != nil {
		t.Fatalf("unknown field rejected: %v", err)
	}
}

func TestDecodeStatusRejects(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{"unknown status", payload(`"status":"DONE","attempts":0`)},
		{"empty status", payload(`"attempts":0`)},
		{"lowercase status", payload(`"status":"indexed","attempts":0`)},
		{"unknown reason", payload(`"status":"FAILED","attempts":0,"retryable":false,"reason":"OUT_OF_COFFEE"`)},
		{"reason on indexed is still validated", payload(`"status":"INDEXED","attempts":0,"reason":"OUT_OF_COFFEE"`)},
		{"failed without retryable", payload(`"status":"FAILED","attempts":0,"reason":"INTERNAL"`)},
		{"failed without reason", payload(`"status":"FAILED","attempts":0,"retryable":true`)},
		{"negative attempts", payload(`"status":"INDEXED","attempts":-1`)},
		{"negative chunk_count", payload(`"status":"INDEXED","attempts":0,"chunk_count":-3`)},
		{"attempts as string", payload(`"status":"INDEXED","attempts":"0"`)},
		{"missing attempts", payload(`"status":"INDEXED"`)},
		{"empty hash", []byte(`{"hash":"","status":"INDEXED","attempts":0,"created_at":"2026-10-14T10:00:00Z","schema_version":1}`)},
		{"missing hash", []byte(`{"status":"INDEXED","attempts":0,"created_at":"2026-10-14T10:00:00Z","schema_version":1}`)},
		{"schema_version 2", []byte(`{"hash":"` + testHash + `","status":"INDEXED","attempts":0,"created_at":"2026-10-14T10:00:00Z","schema_version":2}`)},
		{"schema_version 0", []byte(`{"hash":"` + testHash + `","status":"INDEXED","attempts":0,"created_at":"2026-10-14T10:00:00Z","schema_version":0}`)},
		{"missing schema_version", []byte(`{"hash":"` + testHash + `","status":"INDEXED","attempts":0,"created_at":"2026-10-14T10:00:00Z"}`)},
		{"bad created_at", []byte(`{"hash":"` + testHash + `","status":"INDEXED","attempts":0,"created_at":"yesterday","schema_version":1}`)},
		{"malformed json", []byte(`{"hash":`)},
		{"empty payload", []byte(``)},
		{"json array", []byte(`[]`)},
		{"json null", []byte(`null`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeStatus(tc.body)
			if err == nil {
				t.Fatal("expected error")
			}
			if !errors.Is(err, ErrInvalidStatusEvent) {
				t.Fatalf("error %v does not wrap ErrInvalidStatusEvent", err)
			}
		})
	}
}

func TestDecodeStatusErrorNamesTheProblem(t *testing.T) {
	_, err := DecodeStatus(payload(`"status":"FAILED","attempts":0,"retryable":true,"reason":"OUT_OF_COFFEE"`))
	if err == nil || !strings.Contains(err.Error(), "OUT_OF_COFFEE") {
		t.Fatalf("error = %v, want it to name the unknown reason", err)
	}
}
