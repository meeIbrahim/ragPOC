package httpapi_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"center-service/internal/httpapi"
	"center-service/internal/upload"
)

// marker answers 299 with its name, so a test can tell which handler a request reached.
func marker(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(299)
		_, _ = io.WriteString(w, name)
	}
}

func deps() httpapi.Deps {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return httpapi.Deps{
		// No store: these tests only check routing, and every upload route they hit fails
		// before the store (a bad body or an unknown method).
		Upload:            upload.NewService(nil, nil, logger),
		Ask:               marker("ask"),
		Documents:         marker("documents"),
		Logger:            logger,
		AllowedExtensions: []string{".pdf"},
	}
}

func do(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

type routeCase struct {
	method, path string
	wantCode     int
	wantBody     string // "" = don't check
}

func checkRoutes(t *testing.T, h http.Handler, cases []routeCase) {
	t.Helper()
	for _, c := range cases {
		rec := do(h, c.method, c.path, "{")
		if rec.Code != c.wantCode || (c.wantBody != "" && rec.Body.String() != c.wantBody) {
			t.Errorf("%s %s = %d %q; want %d %q", c.method, c.path, rec.Code, rec.Body.String(), c.wantCode, c.wantBody)
		}
	}
}

func TestRouterWithoutStatic(t *testing.T) {
	checkRoutes(t, httpapi.NewRouter(deps()), []routeCase{
		{http.MethodGet, "/documents", 299, "documents"},
		{http.MethodPost, "/documents", http.StatusMethodNotAllowed, ""},
		{http.MethodPost, "/ask", 299, "ask"},
		{http.MethodGet, "/ask", http.StatusMethodNotAllowed, ""},
		// The upload routes still reach the upload handlers: "{" is rejected as invalid JSON.
		{http.MethodPost, "/upload", http.StatusBadRequest, ""},
		{http.MethodDelete, "/upload/abc", http.StatusMethodNotAllowed, ""},
		// No static_dir, no UI: everything else stays the mux's 404.
		{http.MethodGet, "/", http.StatusNotFound, ""},
	})
}

// With the UI mounted at "/", every API route must still win (deviation 1: GET /ask becomes the
// static handler's 404 instead of the mux's 405).
func TestRouterWithStatic(t *testing.T) {
	d := deps()
	d.Static = marker("static")
	h := httpapi.NewRouter(d) // a pattern conflict would panic here
	checkRoutes(t, h, []routeCase{
		{http.MethodGet, "/", 299, "static"},
		{http.MethodGet, "/assets/app.js", 299, "static"},
		{http.MethodGet, "/documents", 299, "documents"},
		{http.MethodPost, "/documents", 299, "static"}, // the static handler answers 405 itself
		{http.MethodPost, "/ask", 299, "ask"},
		{http.MethodGet, "/ask", 299, "static"},
		{http.MethodPost, "/upload", http.StatusBadRequest, ""},
		{http.MethodDelete, "/upload/abc", http.StatusMethodNotAllowed, ""},
	})
}
