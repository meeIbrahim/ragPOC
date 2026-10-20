package events

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "rewrite the contract fixtures from the Go structs")

func fixtureDir() string {
	return filepath.Join("..", "..", "..", "..", "contracts", "events", "fixtures")
}

// ingestFixtures are marshalled by encoding/json, exactly as the outbox worker publishes them.
// They differ in created_at precision because Go trims trailing zeros, so the Python parser
// has to accept 0, 3 and 9 fractional digits.
func ingestFixtures() []struct {
	file          string
	event         StreamIngestionEvent
	wantCreatedAt string
} {
	base := StreamIngestionEvent{
		ID:            "9b2c0f1e-0000-4000-8000-0000000000a1",
		Hash:          testHash,
		Filename:      "report.pdf",
		BucketPath:    "ba/78/" + testHash,
		Size:          1048576,
		Attempts:      0,
		SchemaVersion: 1,
	}
	nanos, millis, whole := base, base, base
	nanos.CreatedAt = time.Date(2026, 10, 14, 10, 0, 0, 123456789, time.UTC)
	millis.CreatedAt = time.Date(2026, 10, 14, 10, 0, 0, 123000000, time.UTC)
	millis.Attempts, millis.Filename = 1, "Notes.TXT"
	whole.CreatedAt = time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	whole.Attempts, whole.Filename = 2, "README.md"

	return []struct {
		file          string
		event         StreamIngestionEvent
		wantCreatedAt string
	}{
		{"ingest_event.json", nanos, "2026-10-14T10:00:00.123456789Z"},
		{"ingest_event_millis.json", millis, "2026-10-14T10:00:00.123Z"},
		{"ingest_event_whole_second.json", whole, "2026-10-14T10:00:00Z"},
	}
}

// The fixtures are the contract: Python tests parse these exact bytes. Run
// `go test ./internal/events -update` after an intentional change to the event.
func TestIngestEventFixtures(t *testing.T) {
	for _, fx := range ingestFixtures() {
		t.Run(fx.file, func(t *testing.T) {
			got, err := json.Marshal(fx.event)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, '\n')
			path := filepath.Join(fixtureDir(), fx.file)

			if *update {
				if err := os.WriteFile(path, got, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read fixture (run with -update to create it): %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("fixture %s is stale; run `go test ./internal/events -update` if the change is intended\n got: %s\nwant: %s", fx.file, got, want)
			}

			// Guards against regenerating blindly after a change to the time format.
			if !strings.Contains(string(got), `"created_at":"`+fx.wantCreatedAt+`"`) {
				t.Errorf("created_at is not %s in %s", fx.wantCreatedAt, got)
			}

			var back StreamIngestionEvent
			if err := json.Unmarshal(want, &back); err != nil {
				t.Fatal(err)
			}
			if !back.CreatedAt.Equal(fx.event.CreatedAt) || back.Hash != fx.event.Hash ||
				back.Attempts != fx.event.Attempts || back.BucketPath != fx.event.BucketPath {
				t.Errorf("round trip mismatch: %+v", back)
			}
		})
	}
}

// JSON field names are part of the contract with the Python service.
func TestIngestEventFieldNames(t *testing.T) {
	raw, err := json.Marshal(ingestFixtures()[0].event)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"id", "hash", "filename", "bucket_path", "size", "attempts", "created_at", "schema_version"} {
		if _, ok := m[key]; !ok {
			t.Errorf("payload lacks %q", key)
		}
	}
	if len(m) != 8 {
		t.Errorf("payload has %d fields, PRD 4.1 lists 8: %v", len(m), m)
	}
}
