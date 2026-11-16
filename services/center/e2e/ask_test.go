//go:build e2e

package e2e

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"center-service/internal/ask"
)

// unreachableBound is how fast an ask must fail when rag-server is not running: the dial fails
// at once, so anything near [go.ask].deadline means the call waited instead of failing.
const unreachableBound = 30 * time.Second

// postAsk sends a POST /ask and returns the status, content type and whole body. The client
// timeout allows the full [go.ask].deadline, so a hung stream fails on its own assertion.
func postAsk(t *testing.T, body string) (int, string, string) {
	t.Helper()
	client := &http.Client{Timeout: cfg.Go.Ask.Deadline.Std() + 10*time.Second}
	resp, err := client.Post(centerURL()+"/ask", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /ask: %v", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /ask body: %v", err)
	}
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(b)
}

// stub mode: indexed. Bad input is a 400 before any stream starts.
func testAskValidation(t *testing.T) {
	code, ctype, body := postAsk(t, `{"question":"   "}`)
	if code != http.StatusBadRequest || strings.HasPrefix(ctype, "text/event-stream") {
		t.Fatalf("blank question: status %d, content type %q, body %q; want a plain 400", code, ctype, body)
	}

	unknown := uuid.NewString()
	code, ctype, body = postAsk(t, `{"question":"What is this?","upload_ids":["`+unknown+`"]}`)
	if code != http.StatusBadRequest || strings.HasPrefix(ctype, "text/event-stream") || !strings.Contains(body, unknown) {
		t.Fatalf("unknown upload id: status %d, content type %q, body %q; want a plain 400 naming the id", code, ctype, body)
	}
}

// stub mode: failed-permanent. Selecting only a FAILED document never searches all documents:
// Go answers "nothing relevant" itself. rag-server is not running in the e2e, so any call to it
// would end in an error event instead of this exact stream.
func testAskShortCircuit(t *testing.T) {
	s := newScenario(t)
	resetStreams(t, s.rdb)

	id := upload(t, "ask-failed.txt", uniqueContent("ask-short-circuit"))
	if got := waitFinal(t, id, 60*time.Second); got != "FAILED" {
		t.Fatalf("final status = %s, want FAILED", got)
	}
	s.track(hashOf(t, s.db, id))

	code, ctype, body := postAsk(t, `{"question":"What is this?","upload_ids":["`+id+`"]}`)
	want := "event: sources\ndata: {\"sources\":[]}\n\n" +
		"event: delta\ndata: {\"text\":\"" + ask.NoRelevantAnswer + "\"}\n\n" +
		"event: done\ndata: {}\n\n"
	if code != http.StatusOK || ctype != "text/event-stream" || body != want {
		t.Fatalf("status %d, content type %q, body\n%q\nwant\n%q", code, ctype, body, want)
	}
}

// stub mode: indexed. rag-server is not running, so an ask over all documents ends with one
// UNAVAILABLE error event, fast (the dial fails) and well inside [go.ask].deadline.
func testAskUnavailable(t *testing.T) {
	start := time.Now()
	code, ctype, body := postAsk(t, `{"question":"What is this?"}`)
	took := time.Since(start)

	want := "event: error\ndata: {\"code\":\"UNAVAILABLE\",\"message\":\"the answer service is unavailable, try again\"}\n\n"
	if code != http.StatusOK || ctype != "text/event-stream" || body != want {
		t.Fatalf("status %d, content type %q, body\n%q\nwant\n%q", code, ctype, body, want)
	}
	if took >= unreachableBound {
		t.Errorf("took %v, want an unreachable rag-server to fail within %v, not wait toward [go.ask].deadline", took, unreachableBound)
	}
}
