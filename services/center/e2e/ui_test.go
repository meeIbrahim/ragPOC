//go:build e2e

package e2e

import (
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"
)

var scriptAsset = regexp.MustCompile(`src="(/assets/[^"]+\.js)"`)

// compose-demo D4: the center image serves the built UI from static_dir="/srv/web". The index is
// revalidated on every load, so a rebuilt UI shows after a reload; its first script must load too.
func testUIServed(t *testing.T) {
	resp, err := httpClient.Get(centerURL() + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read GET / body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / = %d; want 200: the image must carry the UI at static_dir", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("GET / Content-Type = %q; want text/html", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("GET / Cache-Control = %q; want no-cache so a rebuilt UI shows after a reload", cc)
	}
	if !strings.Contains(string(body), "<title>Pucho</title>") {
		t.Errorf("GET / does not contain <title>Pucho</title>; is it the built SPA?")
	}

	m := scriptAsset.FindStringSubmatch(string(body))
	if m == nil {
		t.Fatalf("GET / references no /assets/*.js script")
	}
	asset, err := httpClient.Get(centerURL() + m[1])
	if err != nil {
		t.Fatalf("GET %s: %v", m[1], err)
	}
	io.Copy(io.Discard, asset.Body)
	asset.Body.Close()
	if asset.StatusCode != http.StatusOK {
		t.Errorf("GET %s = %d; want 200: the index points at a script the image doesn't have", m[1], asset.StatusCode)
	}
}
