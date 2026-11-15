package ask

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"time"

	"center-service/internal/domain"
)

// maxBodyBytes caps a POST /ask body; a question plus max_document_ids upload ids fit easily.
const maxBodyBytes = 64 << 10

// Asker streams an answer from the RAG service (ragclient.Client). The iterator yields the
// sources part first, then deltas, and at most one error, last.
type Asker interface {
	AskStream(ctx context.Context, query string, hashes []string) iter.Seq2[domain.AnswerPart, error]
}

// Fixed error messages: text from the RAG service never reaches the browser.
const (
	msgUnavailable  = "the answer service is unavailable, try again"
	msgShuttingDown = "server shutting down"
	msgInvalidQuery = "the answer service rejected the question"
	msgInternal     = "internal error"
)

// Outcomes in the per-ask log line.
const (
	outcomeDone       = "done"
	outcomeClientGone = "client_gone"
	outcomeShutdown   = "shutdown"
)

type Handlers struct {
	svc      *Service
	asker    Asker
	shutdown context.Context
	logger   *slog.Logger
	now      func() time.Time
}

// NewHandlers builds the POST /ask handler. shutdown is cancelled when the HTTP server starts
// shutting down; open streams then end at once with an error event, so Shutdown does not wait
// for answers to finish.
func NewHandlers(svc *Service, asker Asker, shutdown context.Context, logger *slog.Logger) *Handlers {
	return &Handlers{svc: svc, asker: asker, shutdown: shutdown, logger: logger, now: time.Now}
}

type AskRequest struct {
	Question  string   `json:"question"`
	UploadIDs []string `json:"upload_ids"`
}

// Ask validates the request (400/500 before any stream), then streams the answer as
// server-sent events: sources, deltas, then done, or one error event.
func (h *Handlers) Ask(w http.ResponseWriter, r *http.Request) {
	start := h.now()
	req, ok := decodeAskRequest(w, r)
	if !ok {
		return
	}
	plan, err := h.svc.Prepare(r.Context(), req.Question, req.UploadIDs)
	if err != nil {
		var bad *BadRequestError
		if errors.As(err, &bad) {
			http.Error(w, bad.Message, http.StatusBadRequest)
			return
		}
		h.logger.Error("ask: prepare failed", "err", err)
		http.Error(w, msgInternal, http.StatusInternalServerError)
		return
	}

	sse := startSSE(w)
	var outcome string
	if plan.ShortCircuit {
		outcome = h.shortCircuit(sse)
	} else {
		outcome = h.stream(r.Context(), sse, req.Question, plan.Hashes)
	}
	h.logger.Info("ask",
		"selected", len(req.UploadIDs),
		"hashes", len(plan.Hashes),
		"short_circuit", plan.ShortCircuit,
		"outcome", outcome,
		"duration_ms", h.now().Sub(start).Milliseconds(),
	)
}

func decodeAskRequest(w http.ResponseWriter, r *http.Request) (AskRequest, bool) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	var req AskRequest
	if err := dec.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "request body too large", http.StatusBadRequest)
		} else {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
		}
		return AskRequest{}, false
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		http.Error(w, "invalid JSON body: one object expected", http.StatusBadRequest)
		return AskRequest{}, false
	}
	return req, true
}

// shortCircuit answers a selection with no INDEXED document the way the RAG service answers
// a search with no hits, without calling it.
func (h *Handlers) shortCircuit(sse *sseWriter) string {
	for _, ev := range []struct {
		name    string
		payload any
	}{
		{"sources", sourcesPayload{Sources: []sourcePayload{}}},
		{"delta", deltaPayload{Text: NoRelevantAnswer}},
		{"done", struct{}{}},
	} {
		if err := sse.event(ev.name, ev.payload); err != nil {
			return outcomeClientGone
		}
	}
	return outcomeDone
}

// stream relays the answer. Its context ends when the client leaves or the server shuts down;
// either cancels the RAG call. A failed write means the client is gone: returning stops the
// iterator, which cancels the call too.
func (h *Handlers) stream(reqCtx context.Context, sse *sseWriter, question string, hashes []string) string {
	ctx, cancel := context.WithCancel(reqCtx)
	defer cancel()
	stopOnShutdown := context.AfterFunc(h.shutdown, cancel)
	defer stopOnShutdown()

	gotSources := false
	for part, err := range h.asker.AskStream(ctx, question, hashes) {
		if err != nil {
			return h.fail(sse, err)
		}
		var werr error
		switch {
		case !gotSources:
			gotSources = true
			werr = sse.event("sources", toSourcesPayload(part.Sources))
		case part.Delta != "":
			werr = sse.event("delta", deltaPayload{Text: part.Delta})
		}
		if werr != nil {
			return outcomeClientGone
		}
	}
	if !gotSources {
		// The RAG service always sends the sources first (PRD L16); an empty stream breaks it.
		h.logger.Error("ask: answer stream ended before its sources")
		return h.writeError(sse, "INTERNAL", msgInternal)
	}
	if err := sse.event("done", struct{}{}); err != nil {
		return outcomeClientGone
	}
	return outcomeDone
}

// fail ends the stream for an iterator error. A cancel caused by shutdown gets an error
// event; a cancel caused by the client leaving gets nothing, since nobody is listening.
func (h *Handlers) fail(sse *sseWriter, err error) string {
	switch {
	case errors.Is(err, context.Canceled) && h.shutdown.Err() != nil:
		if werr := sse.event("error", errorPayload{Code: "UNAVAILABLE", Message: msgShuttingDown}); werr != nil {
			return outcomeClientGone
		}
		return outcomeShutdown
	case errors.Is(err, context.Canceled):
		return outcomeClientGone
	case errors.Is(err, domain.ErrRAGUnavailable):
		h.logger.Warn("ask: rag service unavailable", "err", err)
		return h.writeError(sse, "UNAVAILABLE", msgUnavailable)
	case errors.Is(err, domain.ErrInvalidQuery):
		h.logger.Warn("ask: rag service rejected the query", "err", err)
		return h.writeError(sse, "INVALID_ARGUMENT", msgInvalidQuery)
	default:
		h.logger.Error("ask: answer stream failed", "err", err)
		return h.writeError(sse, "INTERNAL", msgInternal)
	}
}

// writeError sends the final error event; the outcome is its code.
func (h *Handlers) writeError(sse *sseWriter, code, message string) string {
	if err := sse.event("error", errorPayload{Code: code, Message: message}); err != nil {
		return outcomeClientGone
	}
	return code
}

func toSourcesPayload(in []domain.Source) sourcesPayload {
	out := make([]sourcePayload, 0, len(in))
	for _, s := range in {
		out = append(out, sourcePayload{Filename: s.Filename, Snippet: s.Snippet, Score: s.Score})
	}
	return sourcesPayload{Sources: out}
}
