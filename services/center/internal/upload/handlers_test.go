package upload_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"center-service/internal/domain"
	"center-service/internal/httpapi"
	"center-service/internal/upload"
)

type fakeStore struct {
	created []*domain.Intent
}

func (f *fakeStore) CreateIntent(_ context.Context, i *domain.Intent) error {
	f.created = append(f.created, i)
	return nil
}
func (f *fakeStore) GetIntent(context.Context, string) (*domain.Intent, error) {
	return nil, domain.ErrIntentNotFound
}
func (f *fakeStore) UpdateIntentToUploaded(context.Context, string, *time.Time) (int64, error) {
	return 0, nil
}
func (f *fakeStore) UpdateIntentToExpired(context.Context, string, *time.Time) (int64, error) {
	return 0, nil
}
func (f *fakeStore) GetDocumentByHash(context.Context, string) (*domain.Document, error) {
	return nil, nil
}

type fakeBlob struct {
	presigns int
}

func (f *fakeBlob) PresignedUploadURL(_ context.Context, objectPath string, _ time.Duration) (*url.URL, error) {
	f.presigns++
	return &url.URL{Scheme: "http", Host: "minio:9000", Path: "/upload-bucket/" + objectPath}, nil
}

func newRouter(t *testing.T) (http.Handler, *fakeStore, *fakeBlob) {
	t.Helper()
	store, blob := &fakeStore{}, &fakeBlob{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := upload.NewService(store, blob, logger)
	return httpapi.NewRouter(svc, logger, []string{".pdf", ".txt", ".md"}), store, blob
}

func postUpload(h http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCreateUploadExtension(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		wantCode int
	}{
		{"pdf", "report.pdf", http.StatusCreated},
		{"txt", "notes.txt", http.StatusCreated},
		{"md", "README.md", http.StatusCreated},
		{"uppercase extension", "REPORT.PDF", http.StatusCreated},
		{"mixed case extension", "Report.Pdf", http.StatusCreated},
		{"dotted name", "v1.2.final.txt", http.StatusCreated},
		{"docx", "report.docx", http.StatusBadRequest},
		{"no extension", "report", http.StatusBadRequest},
		{"trailing dot", "report.", http.StatusBadRequest},
		{"double extension ends in exe", "report.pdf.exe", http.StatusBadRequest},
		{"extension only in directory part", "a.pdf/report", http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, store, blob := newRouter(t)
			rec := postUpload(h, `{"filename":"`+tc.filename+`"}`)
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.wantCode, rec.Body.String())
			}
			rejected := tc.wantCode == http.StatusBadRequest
			if rejected && (blob.presigns != 0 || len(store.created) != 0) {
				t.Fatalf("rejected upload reached storage: presigns=%d intents=%d", blob.presigns, len(store.created))
			}
			if !rejected && (blob.presigns != 1 || len(store.created) != 1) {
				t.Fatalf("accepted upload: presigns=%d intents=%d, want 1 each", blob.presigns, len(store.created))
			}
		})
	}
}

func TestCreateUploadRejectionMessageNamesExtension(t *testing.T) {
	h, _, _ := newRouter(t)
	rec := postUpload(h, `{"filename":"report.docx"}`)
	if !strings.Contains(rec.Body.String(), "unsupported file extension: .docx") {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

func TestCreateUploadKeepsOriginalFilename(t *testing.T) {
	h, store, _ := newRouter(t)
	if rec := postUpload(h, `{"filename":"REPORT.PDF"}`); rec.Code != http.StatusCreated {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := store.created[0].FileName; got != "REPORT.PDF" {
		t.Fatalf("stored filename = %q, want the original case preserved", got)
	}
}

func TestCreateUploadBadInput(t *testing.T) {
	for name, body := range map[string]string{
		"invalid json":   `{"filename":`,
		"empty filename": `{"filename":""}`,
	} {
		t.Run(name, func(t *testing.T) {
			h, _, blob := newRouter(t)
			if rec := postUpload(h, body); rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
			if blob.presigns != 0 {
				t.Fatal("bad input reached storage")
			}
		})
	}
}
