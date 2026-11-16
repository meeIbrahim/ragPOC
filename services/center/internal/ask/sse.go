package ask

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Event payloads. Every data: line is one line of JSON (json.Marshal escapes newlines).
type sourcesPayload struct {
	Sources []sourcePayload `json:"sources"`
}

// sourcePayload is what the browser sees of a source: no hash and no upload id (D3).
type sourcePayload struct {
	Filename string  `json:"filename"`
	Snippet  string  `json:"snippet"`
	Score    float32 `json:"score"`
}

type deltaPayload struct {
	Text string `json:"text"`
}

type errorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// sseWriter writes server-sent events and flushes each one, so the browser sees every delta
// as it arrives.
type sseWriter struct {
	w  http.ResponseWriter
	rc *http.ResponseController
}

// startSSE sends the stream headers and the 200. After this, failures can only be reported as
// an event.
func startSSE(w http.ResponseWriter) *sseWriter {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	// Stops a reverse proxy (nginx) from buffering the stream if one is added later.
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	s := &sseWriter{w: w, rc: http.NewResponseController(w)}
	// A failed first flush is not fatal here: the first event() flush reports it, and the
	// handler treats any write or flush error as the client being gone.
	_ = s.rc.Flush()
	return s
}

func (s *sseWriter) event(name string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("sse: encode %s: %w", name, err)
	}
	if _, err := fmt.Fprintf(s.w, "event: %s\ndata: %s\n\n", name, data); err != nil {
		return fmt.Errorf("sse: write %s: %w", name, err)
	}
	if err := s.rc.Flush(); err != nil {
		return fmt.Errorf("sse: flush %s: %w", name, err)
	}
	return nil
}
