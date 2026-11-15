package ask

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"center-service/internal/domain"
)

const secretHash = "c0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ffee0001"

type harness struct {
	h        *Handlers
	asker    *fakeAsker
	store    *fakeStore
	logs     *bytes.Buffer
	shutdown context.CancelFunc
}

func newHarness(t *testing.T, asker *fakeAsker) *harness {
	t.Helper()
	store := &fakeStore{rows: map[string]domain.UploadRef{
		"up-indexed":    indexed("up-indexed", secretHash),
		"up-processing": {UploadID: "up-processing", Hash: "h2", DocStatus: domain.DocumentProcessing},
	}}
	logs := &bytes.Buffer{}
	shutdownCtx, shutdown := context.WithCancel(context.Background())
	t.Cleanup(shutdown)
	h := NewHandlers(NewService(store, Limits{MaxQueryChars: 50, MaxDocumentIDs: 3}), asker, shutdownCtx,
		slog.New(slog.NewJSONHandler(logs, nil)))
	return &harness{h: h, asker: asker, store: store, logs: logs, shutdown: shutdown}
}

func askRequest(ctx context.Context, body string) *http.Request {
	return httptest.NewRequestWithContext(ctx, http.MethodPost, "/ask", strings.NewReader(body))
}

func (hs *harness) do(body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	hs.h.Ask(rec, askRequest(context.Background(), body))
	return rec
}

type sseEvent struct {
	name string
	data string
}

// parseSSE splits a body into events and fails on anything that is not `event:` + one `data:`
// line + a blank line.
func parseSSE(t *testing.T, body string) []sseEvent {
	t.Helper()
	if body == "" {
		return nil
	}
	if !strings.HasSuffix(body, "\n\n") {
		t.Fatalf("stream does not end with a complete event: %q", body)
	}
	var out []sseEvent
	for _, block := range strings.Split(strings.TrimSuffix(body, "\n\n"), "\n\n") {
		lines := strings.Split(block, "\n")
		if len(lines) != 2 || !strings.HasPrefix(lines[0], "event: ") || !strings.HasPrefix(lines[1], "data: ") {
			t.Fatalf("malformed event %q", block)
		}
		data := strings.TrimPrefix(lines[1], "data: ")
		if !json.Valid([]byte(data)) {
			t.Fatalf("event data is not JSON: %q", data)
		}
		out = append(out, sseEvent{strings.TrimPrefix(lines[0], "event: "), data})
	}
	return out
}

func eventNames(evs []sseEvent) string {
	names := make([]string, len(evs))
	for i, e := range evs {
		names[i] = e.name
	}
	return strings.Join(names, ",")
}

func errorEvent(t *testing.T, evs []sseEvent) errorPayload {
	t.Helper()
	if len(evs) == 0 || evs[len(evs)-1].name != "error" {
		t.Fatalf("events %s: want a final error event", eventNames(evs))
	}
	for _, e := range evs[:len(evs)-1] {
		if e.name == "error" || e.name == "done" {
			t.Fatalf("events %s: error must be the only terminal event", eventNames(evs))
		}
	}
	var p errorPayload
	if err := json.Unmarshal([]byte(evs[len(evs)-1].data), &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestAskStreamsSourcesDeltasDone(t *testing.T) {
	asker := newFakeAsker(
		sources(domain.Source{Hash: secretHash, Filename: "refunds.pdf", Snippet: "Refunds within 30 days", Score: 0.71}),
		delta("Refunds are "), delta(""), delta("accepted "), delta("within 30 days."),
	)
	hs := newHarness(t, asker)
	rec := hs.do(`{"question":"What is the refund window?","upload_ids":["up-indexed"]}`)

	if rec.Code != 200 || rec.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("status %d, content type %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	body := rec.Body.String()
	evs := parseSSE(t, body)
	if got := eventNames(evs); got != "sources,delta,delta,delta,done" {
		t.Fatalf("events = %s, want sources, 3 deltas (empty one skipped), done", got)
	}
	if evs[0].data != `{"sources":[{"filename":"refunds.pdf","snippet":"Refunds within 30 days","score":0.71}]}` {
		t.Errorf("sources data = %s", evs[0].data)
	}
	if evs[1].data != `{"text":"Refunds are "}` || evs[4].data != `{}` {
		t.Errorf("delta/done data = %s / %s", evs[1].data, evs[4].data)
	}
	if strings.Contains(body, secretHash) {
		t.Error("response contains a content hash: hashes must never leave Go")
	}
	if len(asker.hashes) != 1 || asker.hashes[0] != secretHash {
		t.Errorf("asker got hashes %v, want the selected INDEXED hash", asker.hashes)
	}
}

func TestAskEmptySelectionSearchesAll(t *testing.T) {
	asker := newFakeAsker(sources())
	hs := newHarness(t, asker)
	rec := hs.do(`{"question":"q?"}`)
	if got := eventNames(parseSSE(t, rec.Body.String())); got != "sources,done" {
		t.Fatalf("events = %s", got)
	}
	if !strings.Contains(rec.Body.String(), `{"sources":[]}`) {
		t.Errorf("empty sources must be [], body %q", rec.Body.String())
	}
	if asker.callCount() != 1 || len(asker.hashes) != 0 {
		t.Errorf("asker calls=%d hashes=%v, want one call with no hashes (search all)", asker.callCount(), asker.hashes)
	}
}

func TestAskRejectsBadInputBeforeStreaming(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"invalid JSON", `{"question":`},
		{"unknown field", `{"question":"q?","user_id":"x"}`},
		{"trailing data", `{"question":"q?"} {"question":"again"}`},
		{"oversized body", `{"question":"` + strings.Repeat("a", maxBodyBytes) + `"}`},
		{"empty question", `{"question":""}`},
		{"whitespace question (E1)", `{"question":"   "}`},
		{"question too long (E2)", `{"question":"` + strings.Repeat("é", 51) + `"}`},
		{"too many ids", `{"question":"q?","upload_ids":["a","b","c","d"]}`},
		{"unknown id (E3)", `{"question":"q?","upload_ids":["up-indexed","nope"]}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			asker := newFakeAsker(sources())
			hs := newHarness(t, asker)
			rec := hs.do(tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
			if ct := rec.Header().Get("Content-Type"); strings.HasPrefix(ct, "text/event-stream") {
				t.Fatalf("bad input started a stream (content type %q)", ct)
			}
			if asker.callCount() != 0 {
				t.Fatal("asker called for bad input: validation must happen before the RAG service")
			}
		})
	}
}

func TestAskUnknownIDNamedInBody(t *testing.T) {
	hs := newHarness(t, newFakeAsker(sources()))
	rec := hs.do(`{"question":"q?","upload_ids":["nope-123"]}`)
	if rec.Code != 400 || !strings.Contains(rec.Body.String(), "nope-123") {
		t.Fatalf("status %d body %q, want 400 naming the unknown id", rec.Code, rec.Body.String())
	}
}

func TestAskStoreErrorIs500(t *testing.T) {
	asker := newFakeAsker(sources())
	hs := newHarness(t, asker)
	hs.store.err = errors.New("disk on fire")
	rec := hs.do(`{"question":"q?","upload_ids":["up-indexed"]}`)
	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "disk on fire") {
		t.Fatalf("status %d body %q, want 500 with a generic body", rec.Code, rec.Body.String())
	}
	if asker.callCount() != 0 {
		t.Fatal("asker called after a store error")
	}
}

// E4 / R6: nothing INDEXED in the selection never calls the RAG service.
func TestAskShortCircuit(t *testing.T) {
	asker := newFakeAsker(sources())
	hs := newHarness(t, asker)
	rec := hs.do(`{"question":"q?","upload_ids":["up-processing"]}`)
	if asker.callCount() != 0 {
		t.Fatal("selection had no INDEXED document but Asker was called: an empty document_ids searches everything")
	}
	want := "event: sources\ndata: {\"sources\":[]}\n\n" +
		"event: delta\ndata: {\"text\":\"" + NoRelevantAnswer + "\"}\n\n" +
		"event: done\ndata: {}\n\n"
	if rec.Code != 200 || rec.Body.String() != want {
		t.Fatalf("status %d body\n%q\nwant\n%q", rec.Code, rec.Body.String(), want)
	}
}

func TestAskErrorMapping(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode string
	}{
		{"rag unavailable (E8, E12)", fmt.Errorf("%w: Unavailable: groq said: secret detail", domain.ErrRAGUnavailable), "UNAVAILABLE"},
		{"invalid query (E13)", fmt.Errorf("%w: secret detail", domain.ErrInvalidQuery), "INVALID_ARGUMENT"},
		{"anything else", errors.New("secret detail"), "INTERNAL"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			asker := newFakeAsker(sources(), delta("partial"))
			asker.endErr = tc.err
			hs := newHarness(t, asker)
			rec := hs.do(`{"question":"q?"}`)
			evs := parseSSE(t, rec.Body.String())
			if got := eventNames(evs); got != "sources,delta,error" {
				t.Fatalf("events = %s, want deltas kept then one error (E9)", got)
			}
			p := errorEvent(t, evs)
			if p.Code != tc.wantCode {
				t.Errorf("code = %s, want %s", p.Code, tc.wantCode)
			}
			if p.Message == "" || strings.Contains(rec.Body.String(), "secret detail") {
				t.Errorf("message %q: must be a fixed, non-empty string; service error text never reaches the browser", p.Message)
			}
		})
	}
}

// runBlocking starts an ask whose stream blocks after the sources, and returns once the fake
// is waiting. done closes when the handler returns.
func runBlocking(t *testing.T, hs *harness, ctx context.Context) (rec *httptest.ResponseRecorder, done chan struct{}) {
	t.Helper()
	rec = httptest.NewRecorder()
	done = make(chan struct{})
	go func() {
		defer close(done)
		hs.h.Ask(rec, askRequest(ctx, `{"question":"q?"}`))
	}()
	select {
	case <-hs.asker.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("fake asker never started streaming")
	}
	return rec, done
}

func waitDone(t *testing.T, done chan struct{}, why string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("handler still running after %s", why)
	}
}

// E10: a closed connection cancels the RAG call and writes nothing more.
func TestAskClientDisconnect(t *testing.T) {
	asker := newFakeAsker(sources())
	asker.block = true
	hs := newHarness(t, asker)
	ctx, cancel := context.WithCancel(context.Background())
	rec, done := runBlocking(t, hs, ctx)

	cancel()
	waitDone(t, done, "the client disconnected")
	if asker.context().Err() == nil {
		t.Error("asker context still live after the client left: Groq would keep generating")
	}
	if got := eventNames(parseSSE(t, rec.Body.String())); got != "sources" {
		t.Errorf("events = %s, want nothing written after the disconnect", got)
	}
	if !strings.Contains(hs.logs.String(), `"outcome":"client_gone"`) {
		t.Errorf("log %s: want outcome client_gone", hs.logs.String())
	}
}

// E11: shutdown ends open streams with an error and cancels the RAG call.
func TestAskShutdown(t *testing.T) {
	asker := newFakeAsker(sources())
	asker.block = true
	hs := newHarness(t, asker)
	rec, done := runBlocking(t, hs, context.Background())

	hs.shutdown()
	waitDone(t, done, "server shutdown")
	if asker.context().Err() == nil {
		t.Error("asker context still live after shutdown")
	}
	p := errorEvent(t, parseSSE(t, rec.Body.String()))
	if p.Code != "UNAVAILABLE" || p.Message != "server shutting down" {
		t.Errorf("error = %+v, want UNAVAILABLE / server shutting down", p)
	}
	if !strings.Contains(hs.logs.String(), `"outcome":"shutdown"`) {
		t.Errorf("log %s: want outcome shutdown", hs.logs.String())
	}
}

// R12: one line per ask with counts and outcome, never the question, snippets or answer.
func TestAskLogLine(t *testing.T) {
	asker := newFakeAsker(sources(domain.Source{Hash: secretHash, Filename: "f.pdf", Snippet: "SNIPPET-TEXT"}), delta("ANSWER-TEXT"))
	hs := newHarness(t, asker)
	hs.do(`{"question":"QUESTION-TEXT","upload_ids":["up-indexed","up-processing"]}`)

	lines := strings.Split(strings.TrimSpace(hs.logs.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d log lines, want 1: %s", len(lines), hs.logs.String())
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]any{"selected": 2.0, "hashes": 1.0, "short_circuit": false, "outcome": "done"} {
		if rec[k] != want {
			t.Errorf("log %s = %v, want %v", k, rec[k], want)
		}
	}
	if _, ok := rec["duration_ms"]; !ok {
		t.Error("log has no duration_ms")
	}
	for _, secret := range []string{"QUESTION-TEXT", "SNIPPET-TEXT", "ANSWER-TEXT", secretHash} {
		if strings.Contains(lines[0], secret) {
			t.Errorf("log contains %q: questions, snippets, answers and hashes stay out of logs", secret)
		}
	}
}
