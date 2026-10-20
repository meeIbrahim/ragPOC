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

func TestDecodeOutcomeIndexed(t *testing.T) {
	ev, err := DecodeStatus(payload(`"status":"INDEXED","attempts":2,"chunk_count":12`))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Status != OutcomeIndexed || ev.Hash != testHash || ev.Attempts != 2 {
		t.Errorf("event = %+v", ev)
	}
	if ev.ChunkCount == nil || *ev.ChunkCount != 12 {
		t.Errorf("ChunkCount = %v", ev.ChunkCount)
	}
	if ev.CreatedAt.IsZero() {
		t.Error("created_at not parsed")
	}
}

func TestDecodeOutcomeIndexedWithoutOptionals(t *testing.T) {
	ev, err := DecodeStatus(payload(`"status":"INDEXED","attempts":0`))
	if err != nil {
		t.Fatal(err)
	}
	if ev.ChunkCount != nil || ev.Retryable != nil || ev.Reason != "" {
		t.Errorf("optionals should be unset: %+v", ev)
	}
}

func TestDecodeOutcomeFailed(t *testing.T) {
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
			if ev.Status != OutcomeFailed || ev.Retryable == nil || *ev.Retryable != tc.retryable || ev.Reason != tc.reason {
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

// encoding/json matches keys case-insensitively. A producer's stray "Status" key must never
// override the real "status", or a FAILED document could be recorded as INDEXED. The JSON
// schema and Python match keys exactly, so Go has to as well.
func TestDecodeStatusMatchesKeysExactly(t *testing.T) {
	ev, err := DecodeStatus(payload(`"status":"FAILED","retryable":true,"reason":"INTERNAL","attempts":0,"Status":"INDEXED","ATTEMPTS":9`))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Status != OutcomeFailed || ev.Attempts != 0 {
		t.Fatalf("a differently cased key overrode the real one: %+v", ev)
	}

	// Only a wrongly cased key present means the real key is absent.
	_, err = DecodeStatus([]byte(`{"hash":"` + testHash + `","STATUS":"INDEXED","attempts":0,"created_at":"2026-10-14T10:00:00Z","schema_version":1}`))
	if !errors.Is(err, ErrInvalidStatusEvent) {
		t.Fatalf("err = %v, want ErrInvalidStatusEvent for a missing exact-case status", err)
	}
}

// JSON Schema (and so a Python producer that passes it) treats 1.0 and 1e0 as the integer 1.
// Discarding such an event would leave the document PROCESSING, so integral values are accepted.
func TestDecodeStatusAcceptsIntegralNumbers(t *testing.T) {
	for _, attempts := range []string{`1`, `1.0`, `1e0`, `1E0`, `0.1e1`, `-0`} {
		ev, err := DecodeStatus(payload(`"status":"INDEXED","attempts":` + attempts))
		if err != nil {
			t.Errorf("attempts %s rejected: %v", attempts, err)
			continue
		}
		want := 1
		if attempts == `-0` {
			want = 0
		}
		if ev.Attempts != want {
			t.Errorf("attempts %s = %d, want %d", attempts, ev.Attempts, want)
		}
	}
	ev, err := DecodeStatus(payload(`"status":"INDEXED","attempts":0,"chunk_count":2.0`))
	if err != nil || ev.ChunkCount == nil || *ev.ChunkCount != 2 {
		t.Errorf("chunk_count 2.0: %+v, err %v", ev, err)
	}
	if _, err := DecodeStatus([]byte(`{"hash":"` + testHash + `","status":"INDEXED","attempts":0,"created_at":"2026-10-14T10:00:00Z","schema_version":1.0}`)); err != nil {
		t.Errorf("schema_version 1.0 rejected: %v", err)
	}
}

func TestDecodeStatusRejectsNonIntegralNumbers(t *testing.T) {
	for _, attempts := range []string{`1.5`, `-0.5`, `1e20`, `99999999999999999999`, `1e400`, `true`, `[]`, `{}`} {
		if _, err := DecodeStatus(payload(`"status":"INDEXED","attempts":` + attempts)); !errors.Is(err, ErrInvalidStatusEvent) {
			t.Errorf("attempts %s: err = %v, want ErrInvalidStatusEvent", attempts, err)
		}
	}
}

// null means "absent". The consumer only logs id and created_at, so Go does not require them
// even though the schema does; the handler must not assume CreatedAt is set.
func TestDecodeStatusTreatsNullAsAbsent(t *testing.T) {
	ev, err := DecodeStatus([]byte(`{"hash":"` + testHash + `","status":"INDEXED","attempts":0,"schema_version":1,` +
		`"chunk_count":null,"retryable":null,"reason":null,"detail":null,"id":null,"created_at":null}`))
	if err != nil {
		t.Fatal(err)
	}
	if ev.ChunkCount != nil || ev.Retryable != nil || ev.Reason != "" || ev.ID != "" || !ev.CreatedAt.IsZero() {
		t.Errorf("null fields not treated as absent: %+v", ev)
	}
	for name, body := range map[string]string{
		"attempts null":            `{"hash":"` + testHash + `","status":"INDEXED","attempts":null,"schema_version":1}`,
		"schema_version null":      `{"hash":"` + testHash + `","status":"INDEXED","attempts":0,"schema_version":null}`,
		"status null":              `{"hash":"` + testHash + `","status":null,"attempts":0,"schema_version":1}`,
		"hash null":                `{"hash":null,"status":"INDEXED","attempts":0,"schema_version":1}`,
		"retryable null on FAILED": `{"hash":"` + testHash + `","status":"FAILED","attempts":0,"retryable":null,"reason":"INTERNAL","schema_version":1}`,
	} {
		if _, err := DecodeStatus([]byte(body)); !errors.Is(err, ErrInvalidStatusEvent) {
			t.Errorf("%s: err = %v, want ErrInvalidStatusEvent", name, err)
		}
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
