package events

import (
	"errors"
	"os"
	"path/filepath"
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

func TestInvalidStatusFixtures(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(fixtureDir(), "invalid", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) < 6 {
		t.Fatalf("expected at least 6 invalid fixtures, found %d", len(files))
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			raw, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeStatus(raw); !errors.Is(err, ErrInvalidStatusEvent) {
				t.Fatalf("DecodeStatus error = %v, want ErrInvalidStatusEvent", err)
			}
		})
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
