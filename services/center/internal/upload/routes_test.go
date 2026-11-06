package upload_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The root router mounts the upload routes under /upload. Each case proves the request
// reached the intended handler: the fake store knows no intent, so the handler answers with
// its own "upload intent not found", which differs from the mux's "404 page not found".
func TestRouterReachesEachUploadHandler(t *testing.T) {
	h, _, _ := newRouter(t)
	cases := []struct {
		method, path string
		status       int
		body         string
	}{
		{http.MethodPost, "/upload", http.StatusCreated, `"upload_url"`},
		{http.MethodPost, "/upload/abc/confirm", http.StatusNotFound, "upload intent not found"},
		{http.MethodGet, "/upload/abc", http.StatusNotFound, "upload intent not found"},
	}
	for _, c := range cases {
		t.Run(c.method+" "+c.path, func(t *testing.T) {
			var body *strings.Reader
			if c.path == "/upload" {
				body = strings.NewReader(`{"filename":"a.pdf"}`)
			} else {
				body = strings.NewReader("")
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(c.method, c.path, body))

			if rec.Code != c.status || !strings.Contains(rec.Body.String(), c.body) {
				t.Errorf("%s %s = %d %q; want %d containing %q (the request must reach its handler, prefix stripped)",
					c.method, c.path, rec.Code, rec.Body.String(), c.status, c.body)
			}
		})
	}
}

// StripPrefix turns a bare /upload into an empty path, which an inner mux would 301 to "/".
// The root router must serve POST /upload itself.
func TestRouterDoesNotRedirectTheBareUploadPath(t *testing.T) {
	h, _, _ := newRouter(t)
	rec := postUpload(h, `{"filename":"a.pdf"}`)
	if rec.Code == http.StatusMovedPermanently || rec.Header().Get("Location") != "" {
		t.Errorf("POST /upload = %d (Location %q); want it served directly, not redirected", rec.Code, rec.Header().Get("Location"))
	}
}

func TestRouterRejectsTheWrongMethod(t *testing.T) {
	h, _, _ := newRouter(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/upload/abc/confirm", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /upload/abc/confirm = %d; want 405: confirm is POST only", rec.Code)
	}
}
