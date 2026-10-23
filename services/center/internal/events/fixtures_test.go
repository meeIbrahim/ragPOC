package events

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The valid status fixtures are hand-written: Python's serializer tests must reproduce them,
// and Go must accept every one. An unlisted file fails the test, so a new fixture cannot be
// added without stating what it means.
func TestValidStatusFixtures(t *testing.T) {
	tests := []struct {
		file       string
		status     Outcome
		attempts   int
		retryable  *bool
		reason     FailureReason
		chunkCount *int
	}{
		{"status_indexed.json", OutcomeIndexed, 0, nil, "", intPtr(12)},
		{"status_failed_retryable.json", OutcomeFailed, 1, boolPtr(true), ReasonDependencyUnavailable, nil},
		{"status_failed_permanent.json", OutcomeFailed, 0, boolPtr(false), ReasonUnsupportedType, nil},
		{"status_failed_poison.json", OutcomeFailed, 2, boolPtr(false), ReasonPoisonMessage, nil},
	}

	listed := map[string]bool{}
	for _, tc := range tests {
		listed[tc.file] = true
		t.Run(tc.file, func(t *testing.T) {
			ev, err := DecodeStatus(readFixture(t, tc.file))
			if err != nil {
				t.Fatal(err)
			}
			if ev.Status != tc.status || ev.Attempts != tc.attempts || ev.Reason != tc.reason {
				t.Errorf("event = %+v", ev)
			}
			if ev.Hash != testHash {
				t.Errorf("hash = %q", ev.Hash)
			}
			if !samePtr(ev.Retryable, tc.retryable) || !samePtr(ev.ChunkCount, tc.chunkCount) {
				t.Errorf("retryable/chunk_count = %v/%v, want %v/%v", ev.Retryable, ev.ChunkCount, tc.retryable, tc.chunkCount)
			}
		})
	}

	files, err := filepath.Glob(filepath.Join(fixtureDir(), "status_*.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if !listed[filepath.Base(f)] {
			t.Errorf("fixture %s is not covered by this test", filepath.Base(f))
		}
	}
	if len(files) != len(tests) {
		t.Errorf("found %d status fixtures, test lists %d", len(files), len(tests))
	}
}

// Each invalid fixture must be rejected for the reason in its name, so editing one into a
// fixture that is invalid for a different reason does not go unnoticed.
func TestInvalidStatusFixtures(t *testing.T) {
	tests := map[string]string{
		"status_attempts_missing.json":         "attempts must be present",
		"status_attempts_negative.json":        "attempts must be present and >= 0",
		"status_failed_missing_reason.json":    "reason is required",
		"status_failed_missing_retryable.json": "retryable is required",
		"status_hash_missing.json":             "hash is required",
		"status_malformed.json":                "malformed json",
		"status_status_missing.json":           "status is required",
		"status_unknown_reason.json":           `unknown reason "OUT_OF_COFFEE"`,
		"status_unknown_status.json":           `unknown status "DONE"`,
		"status_unknown_version.json":          "unsupported schema_version 2",
	}
	for file, wantReason := range tests {
		t.Run(file, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(fixtureDir(), "invalid", file))
			if err != nil {
				t.Fatal(err)
			}
			_, err = DecodeStatus(raw)
			if !errors.Is(err, ErrInvalidStatusEvent) {
				t.Fatalf("DecodeStatus error = %v, want ErrInvalidStatusEvent", err)
			}
			if !strings.Contains(err.Error(), wantReason) {
				t.Fatalf("rejected for the wrong reason: %v (want %q)", err, wantReason)
			}
		})
	}

	files, err := filepath.Glob(filepath.Join(fixtureDir(), "invalid", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if _, ok := tests[filepath.Base(f)]; !ok {
			t.Errorf("invalid fixture %s is not covered by this test", filepath.Base(f))
		}
	}
	if len(files) != len(tests) {
		t.Errorf("found %d invalid fixtures, test lists %d", len(files), len(tests))
	}
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixtureDir(), name))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func boolPtr(b bool) *bool { return &b }
func intPtr(i int) *int    { return &i }

func samePtr[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
