package webui

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const secret = "TOP-SECRET-CONFIG"

// site builds <tmp>/site with an index, an asset, a sub-directory without an index, one with an
// index, and dotfiles, plus <tmp>/secret.txt beside it that must never be served.
func site(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "site")
	files := map[string]string{
		"index.html":        "<html>app</html>",
		"assets/app.js":     "console.log(1)",
		"docs/index.html":   "<html>docs</html>",
		".env":              "HIDDEN",
		".well-known/thing": "HIDDEN",
		"assets/.map":       "HIDDEN",
	}
	for name, body := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(dir, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "secret.txt"), []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func get(h http.Handler, method, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec
}

func TestServesIndexWithoutCaching(t *testing.T) {
	h := Handler(site(t))
	rec := get(h, http.MethodGet, "/")
	if rec.Code != http.StatusOK || rec.Body.String() != "<html>app</html>" {
		t.Fatalf("GET / = %d %q; want 200 and index.html", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q; want text/html", ct)
	}
	// F5 A7: a rebuilt UI must show after a reload.
	if cc := rec.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("Cache-Control = %q; want no-cache on index.html", cc)
	}
	if rec := get(h, http.MethodHead, "/"); rec.Code != http.StatusOK {
		t.Errorf("HEAD / = %d; want 200", rec.Code)
	}
}

func TestServesAssetsWithDefaultCaching(t *testing.T) {
	rec := get(Handler(site(t)), http.MethodGet, "/assets/app.js")
	if rec.Code != http.StatusOK || rec.Body.String() != "console.log(1)" {
		t.Fatalf("GET /assets/app.js = %d %q; want 200 and the file", rec.Code, rec.Body.String())
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "" {
		t.Errorf("Cache-Control = %q on an asset; want the FileServer default (none)", cc)
	}
}

func TestNotFound(t *testing.T) {
	h := Handler(site(t))
	for _, target := range []string{
		"/missing.js",
		"/empty/", "/empty", // a directory without index.html: never a listing (A6)
		"/assets/",                                    // has files but no index.html
		"/.env", "/.well-known/thing", "/assets/.map", // dotfiles (A6)
	} {
		rec := get(h, http.MethodGet, target)
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s = %d %q; want 404", target, rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "HIDDEN") || strings.Contains(rec.Body.String(), "app.js") {
			t.Errorf("GET %s body %q exposes a file or a listing", target, rec.Body.String())
		}
	}
}

func TestDirectoryWithIndexIsServed(t *testing.T) {
	rec := get(Handler(site(t)), http.MethodGet, "/docs/")
	if rec.Code != http.StatusOK || rec.Body.String() != "<html>docs</html>" {
		t.Errorf("GET /docs/ = %d %q; want its index.html", rec.Code, rec.Body.String())
	}
}

// Deviation 1: the handler sits at "/" for every method, so it answers non-GET itself.
func TestOtherMethodsAre405(t *testing.T) {
	h := Handler(site(t))
	for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		rec := get(h, m, "/")
		if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, HEAD" {
			t.Errorf("%s / = %d Allow=%q; want 405 with Allow: GET, HEAD", m, rec.Code, rec.Header().Get("Allow"))
		}
	}
}

// Mounted the way the router mounts it, traversal never reaches a file outside static_dir. The
// mux may redirect an unclean path; the redirect must stay inside the site.
func TestTraversalStaysInsideTheSite(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/", Handler(site(t)))
	for _, target := range []string{"/../secret.txt", "/%2e%2e/secret.txt", "/..%2fsecret.txt", "/assets/../../secret.txt", "/%2e%2e%2fsecret.txt"} {
		rec := get(mux, http.MethodGet, target)
		if strings.Contains(rec.Body.String(), secret) {
			t.Fatalf("GET %s = %d returned the secret file outside static_dir", target, rec.Code)
		}
		if loc := rec.Header().Get("Location"); loc != "" && (strings.Contains(loc, "..") || !strings.HasPrefix(loc, "/")) {
			t.Errorf("GET %s redirects to %q; want a cleaned path inside the site", target, loc)
		}
		if rec.Code == http.StatusOK {
			t.Errorf("GET %s = 200 %q; want a 404 or a redirect", target, rec.Body.String())
		}
	}
}

// compose-demo D5: the API checks static_dir at startup, so a typo or an image built without the
// UI fails before any worker runs instead of serving 404s.
func TestCheckDir(t *testing.T) {
	valid := site(t)
	indexDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(indexDir, "index.html"), 0o700); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		dir     string
		wantErr string // "" means accepted
	}{
		{"empty means no UI", "", ""},
		{"directory with index.html", valid, ""},
		{"missing directory", filepath.Join(valid, "nope"), "must be an existing directory"},
		{"a file, not a directory", filepath.Join(valid, "index.html"), "must be a directory"},
		{"directory without index.html", t.TempDir(), "must contain index.html"},
		{"index.html is a directory", indexDir, "index.html must be a regular file"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckDir(tc.dir)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("CheckDir(%q) = %v; want it accepted", tc.dir, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) || !strings.Contains(err.Error(), tc.dir) {
				t.Fatalf("CheckDir(%q) = %v; want an error naming the dir and %q", tc.dir, err, tc.wantErr)
			}
		})
	}
}
