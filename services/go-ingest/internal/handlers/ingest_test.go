package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"ragingest/internal/db"
)

var errObjectNotFound = errors.New("object not found")

type fakeStore struct {
	hash string
	err  error
}

func (f *fakeStore) PresignedUploadPolicy(ctx context.Context, objectKey string, expiry time.Duration) (string, map[string]string, error) {
	return "http://minio.local/upload", map[string]string{"key": objectKey}, nil
}

func (f *fakeStore) HashObject(ctx context.Context, objectKey string) (string, error) {
	return f.hash, f.err
}

type fakePublisher struct {
	calls int
	err   error
}

func (f *fakePublisher) Publish(ctx context.Context, hashID, objectPath, contentType string) error {
	f.calls++
	return f.err
}

func newTestHandler(t *testing.T, store *fakeStore, pub *fakePublisher) *IngestHandler {
	t.Helper()
	conn, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &IngestHandler{DB: conn, Storage: store, Publisher: pub}
}

func TestInitReturnsUploadURLAndObjectPath(t *testing.T) {
	h := newTestHandler(t, &fakeStore{}, &fakePublisher{})

	req := httptest.NewRequest(http.MethodPost, "/ingest/init", bytes.NewBufferString(`{"filename":"doc.pdf"}`))
	rec := httptest.NewRecorder()

	h.Init(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp initResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.UploadURL == "" || resp.ObjectPath == "" {
		t.Fatalf("resp = %+v, want non-empty upload_url and object_path", resp)
	}
}

func TestConfirmInsertsAndPublishesNewDocument(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(t, &fakeStore{hash: "abc123"}, pub)

	req := httptest.NewRequest(http.MethodPost, "/ingest/confirm", bytes.NewBufferString(`{"object_path":"objects/abc.pdf"}`))
	rec := httptest.NewRecorder()

	h.Confirm(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	if pub.calls != 1 {
		t.Fatalf("publisher called %d times, want 1", pub.calls)
	}

	doc, err := db.GetByHash(h.DB, "abc123")
	if err != nil || doc == nil {
		t.Fatalf("GetByHash: doc=%+v err=%v", doc, err)
	}
	if !doc.PublishedAt.Valid {
		t.Fatalf("PublishedAt should be set after successful publish, got %+v", doc.PublishedAt)
	}
}

func TestConfirmReturns404WhenObjectMissing(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(t, &fakeStore{err: errObjectNotFound}, pub)

	req := httptest.NewRequest(http.MethodPost, "/ingest/confirm", bytes.NewBufferString(`{"object_path":"objects/missing.pdf"}`))
	rec := httptest.NewRecorder()

	h.Confirm(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if pub.calls != 0 {
		t.Fatalf("publisher should not be called when object is missing, got %d calls", pub.calls)
	}
}

func TestConfirmRejectsDuplicateHash(t *testing.T) {
	pub := &fakePublisher{}
	h := newTestHandler(t, &fakeStore{hash: "abc123"}, pub)
	if err := db.Insert(h.DB, "abc123", "objects/first.pdf"); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/ingest/confirm", bytes.NewBufferString(`{"object_path":"objects/dup.pdf"}`))
	rec := httptest.NewRecorder()

	h.Confirm(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
	if pub.calls != 0 {
		t.Fatalf("publisher should not be called for a duplicate, got %d calls", pub.calls)
	}
}

func TestConfirmSurvivesPublishFailureViaOutbox(t *testing.T) {
	pub := &fakePublisher{err: context.DeadlineExceeded}
	h := newTestHandler(t, &fakeStore{hash: "abc123"}, pub)

	req := httptest.NewRequest(http.MethodPost, "/ingest/confirm", bytes.NewBufferString(`{"object_path":"objects/abc.pdf"}`))
	rec := httptest.NewRecorder()

	h.Confirm(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 even when publish fails (outbox picks it up)", rec.Code)
	}
	doc, err := db.GetByHash(h.DB, "abc123")
	if err != nil || doc == nil {
		t.Fatalf("GetByHash: doc=%+v err=%v", doc, err)
	}
	if doc.PublishedAt.Valid {
		t.Fatalf("PublishedAt should stay unset when publish failed, got %+v", doc.PublishedAt)
	}
}
