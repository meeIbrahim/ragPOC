package upload

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"center-service/internal/domain"
)

// serve sends one request to the upload routes (prefix already stripped, as the root router does).
func serve(store *fakeStore, method, path string) *httptest.ResponseRecorder {
	h := NewHandlers(newTestService(store, &fakeBlob{}), discard, []string{".pdf"}).Routes()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

// ---- POST /{id}/confirm

func TestConfirmHandlerAcceptsWith202(t *testing.T) {
	store := newFakeStore()
	store.put(waiting("i1", now.Add(time.Hour)))

	rec := serve(store, http.MethodPost, "/i1/confirm")

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d; want 202: hashing runs asynchronously after the gate", rec.Code)
	}
	var body ConfirmUploadResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.ID != "i1" || body.FileName != "i1.pdf" || body.Status != "UPLOADED" || body.UploadedAt == nil || !body.UploadedAt.Equal(now) {
		t.Errorf("body = %+v; want id, filename, UPLOADED and uploaded_at", body)
	}
}

func TestConfirmHandlerMapsEachRefusal(t *testing.T) {
	cases := []struct {
		name   string
		seed   func(*fakeStore)
		status int
	}{
		{"expired", func(s *fakeStore) { s.put(waiting("i1", now.Add(-time.Second))) }, http.StatusGone},
		{"failed", func(s *fakeStore) {
			i := waiting("i1", now.Add(time.Hour))
			i.Status = domain.IntentFailed
			s.put(i)
		}, http.StatusConflict},
		{"not found", func(*fakeStore) {}, http.StatusNotFound},
		{"unknown state", func(s *fakeStore) {
			i := waiting("i1", now.Add(time.Hour))
			i.Status = domain.IntentUnknown
			s.put(i)
		}, http.StatusInternalServerError},
		{"store error", func(s *fakeStore) { s.errs["UpdateIntentToUploaded"] = errBoom }, http.StatusInternalServerError},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := newFakeStore()
			c.seed(store)

			rec := serve(store, http.MethodPost, "/i1/confirm")

			if rec.Code != c.status {
				t.Errorf("status = %d; want %d", rec.Code, c.status)
			}
			if c.status == http.StatusInternalServerError && strings.Contains(rec.Body.String(), "boom") {
				t.Errorf("body = %q; an internal error must not leak its cause to the client", rec.Body.String())
			}
		})
	}
}

// ---- GET /{id}

func TestStatusHandlerReturnsTheStatusUncached(t *testing.T) {
	store := newFakeStore()
	i := waiting("i1", now.Add(time.Hour))
	i.Status, i.Hash = domain.IntentHashed, ptr(docHash)
	store.put(i)
	store.documents[docHash] = &domain.Document{Hash: docHash, Status: domain.DocumentIndexed}

	rec := serve(store, http.MethodGet, "/i1")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rec.Code)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q; want no-store: clients poll this, a cached answer would never change", cc)
	}
	var body UploadStatusResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.ID != "i1" || body.Status != "INDEXED" {
		t.Errorf("body = %+v; want id i1 and the document status INDEXED", body)
	}
}

// F2 D3: failure_reason is in the raw body only when the status is FAILED; omitempty keeps it
// out otherwise, and a FAILED row from before migration 0004 has no field either.
func TestStatusHandlerFailureReasonField(t *testing.T) {
	cases := []struct {
		name   string
		seed   func(*fakeStore)
		reason string // "" = the key must be absent
	}{
		{"document failed", func(s *fakeStore) { seedHashed(s, domain.DocumentFailed, ptr("CORRUPT_FILE")) }, "CORRUPT_FILE"},
		{"intent failed", func(s *fakeStore) {
			i := waiting("i1", now.Add(time.Hour))
			i.Status = domain.IntentFailed
			s.put(i)
		}, domain.ReasonUploadFailed},
		{"document failed with NULL reason", func(s *fakeStore) { seedHashed(s, domain.DocumentFailed, nil) }, ""},
		{"processing", func(s *fakeStore) { seedHashed(s, domain.DocumentProcessing, nil) }, ""},
		{"indexed", func(s *fakeStore) { seedHashed(s, domain.DocumentIndexed, nil) }, ""},
		{"expired", func(s *fakeStore) { s.put(waiting("i1", now.Add(-time.Second))) }, ""},
		{"waiting", func(s *fakeStore) { s.put(waiting("i1", now.Add(time.Hour))) }, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := newFakeStore()
			c.seed(store)
			rec := serve(store, http.MethodGet, "/i1")
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d; want 200", rec.Code)
			}
			var raw map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
				t.Fatal(err)
			}
			got, present := raw["failure_reason"]
			if c.reason == "" && present {
				t.Errorf("body %s has failure_reason; want the key absent unless the status is FAILED with a known reason", rec.Body.String())
			}
			if c.reason != "" && got != c.reason {
				t.Errorf("failure_reason = %v in %s; want %q", got, rec.Body.String(), c.reason)
			}
		})
	}
}

func seedHashed(s *fakeStore, status domain.DocumentStatus, reason *string) {
	i := waiting("i1", now.Add(time.Hour))
	i.Status, i.Hash = domain.IntentHashed, ptr(docHash)
	s.put(i)
	s.documents[docHash] = &domain.Document{Hash: docHash, Status: status, FailureReason: reason}
}

func TestStatusHandlerErrors(t *testing.T) {
	cases := []struct {
		name   string
		seed   func(*fakeStore)
		status int
	}{
		{"not found", func(*fakeStore) {}, http.StatusNotFound},
		{"unknown state", func(s *fakeStore) {
			i := waiting("i1", now.Add(time.Hour))
			i.Status = domain.IntentUnknown
			s.put(i)
		}, http.StatusInternalServerError},
		{"document missing", func(s *fakeStore) {
			i := waiting("i1", now.Add(time.Hour))
			i.Status, i.Hash = domain.IntentHashed, ptr(docHash)
			s.put(i)
		}, http.StatusInternalServerError},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := newFakeStore()
			c.seed(store)
			rec := serve(store, http.MethodGet, "/i1")
			if rec.Code != c.status {
				t.Errorf("status = %d; want %d", rec.Code, c.status)
			}
			if c.status == http.StatusInternalServerError && strings.Contains(rec.Body.String(), docHash) {
				t.Errorf("body = %q; an internal error must not leak the document hash to the client", rec.Body.String())
			}
		})
	}
}
