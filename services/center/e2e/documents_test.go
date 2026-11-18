//go:build e2e

package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

// listedDocument is one GET /documents entry. Reason is nil when the key is absent.
type listedDocument struct {
	ID        string   `json:"id"`
	UploadIDs []string `json:"upload_ids"`
	FileName  string   `json:"filename"`
	Status    string   `json:"status"`
	Reason    *string  `json:"failure_reason"`
}

func listDocuments(t *testing.T) []listedDocument {
	t.Helper()
	resp, err := httpClient.Get(centerURL() + "/documents")
	if err != nil {
		t.Fatalf("GET /documents: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /documents = %d: %s", resp.StatusCode, raw)
	}
	// L21: no field may carry a content hash.
	if strings.Contains(string(raw), `"hash`) {
		t.Errorf("GET /documents body has a hash key: %s", raw)
	}
	var body struct {
		Documents []listedDocument `json:"documents"`
		Truncated bool             `json:"truncated"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode GET /documents: %v (%s)", err, raw)
	}
	if body.Truncated {
		t.Fatalf("GET /documents is truncated; the e2e run has far fewer than max_entries documents")
	}
	return body.Documents
}

// assertListed checks that exactly one GET /documents entry holds upload id, with that status
// and failure_reason ("" = the key must be absent), and returns it.
func assertListed(t *testing.T, id, status, reason string) listedDocument {
	t.Helper()
	var found []listedDocument
	for _, d := range listDocuments(t) {
		if slices.Contains(d.UploadIDs, id) {
			found = append(found, d)
		}
	}
	if len(found) != 1 {
		t.Fatalf("upload %s is in %d GET /documents entries %+v; want exactly one", id, len(found), found)
	}
	d := found[0]
	if d.Status != status {
		t.Errorf("listed status of %s = %s; want %s", id, d.Status, status)
	}
	switch {
	case reason == "" && d.Reason != nil:
		t.Errorf("listed %s has failure_reason %q; want none", id, *d.Reason)
	case reason != "" && (d.Reason == nil || *d.Reason != reason):
		t.Errorf("listed failure_reason of %s = %v; want %q", id, d.Reason, reason)
	}
	return d
}

// stub mode: indexed. A second upload of the same content joins the first entry instead of
// adding one.
func testDocumentsList(t *testing.T) {
	s := newScenario(t)
	resetStreams(t, s.rdb)

	content := uniqueContent("documents-list")
	first := upload(t, "documents-first.txt", content)
	if got := waitFinal(t, first, 60*time.Second); got != "INDEXED" {
		t.Fatalf("final status = %s, want INDEXED", got)
	}
	s.track(hashOf(t, s.db, first))
	if d := assertListed(t, first, "INDEXED", ""); d.ID != first || !slices.Equal(d.UploadIDs, []string{first}) {
		t.Fatalf("entry = %+v; want id %s and upload_ids [%s]", d, first, first)
	}

	dup := upload(t, "documents-dup.txt", content)
	// The duplicate follows the document once hashed, so INDEXED means it is linked.
	if got := waitFinal(t, dup, 60*time.Second); got != "INDEXED" {
		t.Fatalf("duplicate final status = %s, want INDEXED", got)
	}
	d := assertListed(t, first, "INDEXED", "")
	if d.ID != first || !slices.Equal(d.UploadIDs, []string{first, dup}) {
		t.Errorf("entry = %+v; want id %s (the oldest upload) and upload_ids [%s %s]", d, first, first, dup)
	}
	if d.FileName != "documents-first.txt" {
		t.Errorf("filename = %q; want the document's name, documents-first.txt", d.FileName)
	}
	assertListed(t, dup, "INDEXED", "") // and in no second entry
}
