package documents

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"center-service/internal/domain"
)

var created = time.Date(2026, 11, 16, 9, 12, 3, 0, time.UTC)

func serve(t *testing.T, lister *fakeLister, maxEntries int) (*httptest.ResponseRecorder, string) {
	t.Helper()
	var logs bytes.Buffer
	h := NewHandlers(lister, maxEntries, slog.New(slog.NewTextHandler(&logs, nil)))
	rec := httptest.NewRecorder()
	h.List(rec, httptest.NewRequest(http.MethodGet, "/documents", nil))
	return rec, logs.String()
}

func decodeRaw(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var raw map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("body %q is not JSON: %v", rec.Body.String(), err)
	}
	return raw
}

func TestListShape(t *testing.T) {
	lister := &fakeLister{entries: []domain.DocumentEntry{
		{ID: "u2", UploadIDs: []string{"u2", "u3"}, FileName: "pods.md", Status: domain.DocumentIndexed, CreatedAt: created},
		{ID: "u1", UploadIDs: []string{"u1"}, FileName: "scan.pdf", Status: domain.DocumentFailed, FailureReason: "EMPTY_TEXT", CreatedAt: created.Add(-time.Minute)},
	}}
	rec, _ := serve(t, lister, 500)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q; want application/json", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q; want no-store: the UI polls this list", cc)
	}
	if lister.gotLimit != 500 {
		t.Errorf("store asked for %d entries; want [go.documents].max_entries = 500", lister.gotLimit)
	}

	want := `{"documents":[` +
		`{"id":"u2","upload_ids":["u2","u3"],"filename":"pods.md","status":"INDEXED","created_at":"2026-11-16T09:12:03Z"},` +
		`{"id":"u1","upload_ids":["u1"],"filename":"scan.pdf","status":"FAILED","failure_reason":"EMPTY_TEXT","created_at":"2026-11-16T09:11:03Z"}` +
		`],"truncated":false}`
	if got := strings.TrimSpace(rec.Body.String()); got != want {
		t.Errorf("body =\n%s\nwant\n%s", got, want)
	}
}

func TestListEmptyIsAnArray(t *testing.T) {
	for name, entries := range map[string][]domain.DocumentEntry{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			rec, _ := serve(t, &fakeLister{entries: entries}, 500)
			if got := strings.TrimSpace(rec.Body.String()); got != `{"documents":[],"truncated":false}` {
				t.Errorf("body = %s; want documents [] (never null) and truncated false", got)
			}
		})
	}
}

func TestListPassesTruncatedThrough(t *testing.T) {
	rec, _ := serve(t, &fakeLister{entries: []domain.DocumentEntry{{ID: "u1", UploadIDs: []string{"u1"}, Status: domain.DocumentIndexed}}, truncated: true}, 1)
	if raw := decodeRaw(t, rec); raw["truncated"] != true {
		t.Errorf("truncated = %v; want true: the store found more entries than max_entries", raw["truncated"])
	}
}

// F2 D3 and L21: the reason only travels with FAILED, and no field ever names the hash.
func TestListFailureReasonOnlyOnFailedAndNoHash(t *testing.T) {
	lister := &fakeLister{entries: []domain.DocumentEntry{
		{ID: "p", UploadIDs: []string{"p"}, Status: domain.DocumentProcessing, FailureReason: "INTERNAL"},
		{ID: "i", UploadIDs: []string{"i"}, Status: domain.DocumentIndexed},
		{ID: "f", UploadIDs: []string{"f"}, Status: domain.DocumentFailed},
	}}
	rec, _ := serve(t, lister, 500)
	body := rec.Body.String()
	if strings.Contains(body, "hash") {
		t.Errorf("body %s mentions a hash; content hashes never leave the server", body)
	}
	docs, _ := decodeRaw(t, rec)["documents"].([]any)
	if len(docs) != 3 {
		t.Fatalf("documents = %v; want 3", docs)
	}
	for _, d := range docs {
		doc := d.(map[string]any)
		if _, ok := doc["failure_reason"]; ok {
			t.Errorf("%v has failure_reason; want none: only a FAILED entry with a known reason carries one", doc)
		}
	}
}

func TestListStoreErrorIs500AndLogged(t *testing.T) {
	rec, logs := serve(t, &fakeLister{err: errors.New("disk I/O error: secret detail")}, 500)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d; want 500", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "secret detail") {
		t.Errorf("body %q leaks the store error to the client", rec.Body.String())
	}
	if !strings.Contains(logs, "level=ERROR") || !strings.Contains(logs, "secret detail") {
		t.Errorf("logs = %q; want an ERROR line with the cause", logs)
	}
}
