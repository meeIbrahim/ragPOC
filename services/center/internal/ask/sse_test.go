package ask

import (
	"net/http/httptest"
	"testing"
)

func TestSSEWriterHeadersAndExactBytes(t *testing.T) {
	rec := httptest.NewRecorder()
	sse := startSSE(rec)
	if err := sse.event("delta", deltaPayload{Text: "line one\nline two \"quoted\""}); err != nil {
		t.Fatal(err)
	}
	if err := sse.event("done", struct{}{}); err != nil {
		t.Fatal(err)
	}

	for k, want := range map[string]string{
		"Content-Type":      "text/event-stream",
		"Cache-Control":     "no-cache",
		"X-Accel-Buffering": "no",
	} {
		if got := rec.Header().Get(k); got != want {
			t.Errorf("header %s = %q, want %q", k, got, want)
		}
	}
	if rec.Code != 200 {
		t.Errorf("status = %d", rec.Code)
	}
	if !rec.Flushed {
		t.Error("events not flushed: the browser would see nothing until the stream ends")
	}
	// A raw newline inside data: would split the event; JSON escapes it so data stays one line.
	want := "event: delta\ndata: {\"text\":\"line one\\nline two \\\"quoted\\\"\"}\n\n" +
		"event: done\ndata: {}\n\n"
	if got := rec.Body.String(); got != want {
		t.Errorf("body =\n%q\nwant\n%q", got, want)
	}
}
