package main

import (
	"bufio"
	"context"
	"io"
	"iter"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"center-service/internal/ask"
	"center-service/internal/domain"
)

type noStore struct{}

func (noStore) ResolveUploads(context.Context, []string) ([]domain.UploadRef, error) { return nil, nil }

// blockingAsker sends the sources, then holds the stream open until its context ends, like a
// long Groq answer.
type blockingAsker struct{ ctxDone chan struct{} }

func (a *blockingAsker) AskStream(ctx context.Context, _ string, _ []string) iter.Seq2[domain.AnswerPart, error] {
	return func(yield func(domain.AnswerPart, error) bool) {
		if !yield(domain.AnswerPart{Sources: []domain.Source{}}, nil) {
			return
		}
		<-ctx.Done()
		close(a.ctxDone)
		yield(domain.AnswerPart{}, ctx.Err())
	}
}

// E11: Shutdown with an open answer stream must end the stream at once (error event, RAG call
// cancelled) instead of waiting out shutdownTimeout.
func TestShutdownEndsOpenAskStreams(t *testing.T) {
	asker := &blockingAsker{ctxDone: make(chan struct{})}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := newServer(func(askShutdown context.Context) http.Handler {
		h := ask.NewHandlers(ask.NewService(noStore{}, ask.Limits{MaxQueryChars: 100, MaxDocumentIDs: 10}), asker, askShutdown, logger)
		mux := http.NewServeMux()
		mux.HandleFunc("POST /ask", h.Ask)
		return mux
	})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(lis) }()

	resp, err := http.Post("http://"+lis.Addr().String()+"/ask", "application/json", strings.NewReader(`{"question":"q?"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	if line, err := reader.ReadString('\n'); err != nil || line != "event: sources\n" {
		t.Fatalf("first line %q, %v: want the sources event before shutting down", line, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	start := time.Now()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v: an open stream held the server until the timeout", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("Shutdown took %v with one open stream, want well under %v", took, shutdownTimeout)
	}
	select {
	case <-asker.ctxDone:
	default:
		t.Error("the ask context was not cancelled: the RAG call would keep running")
	}
	rest, _ := io.ReadAll(reader)
	if !strings.Contains(string(rest), "event: error\ndata: {\"code\":\"UNAVAILABLE\",\"message\":\"server shutting down\"}\n\n") {
		t.Errorf("stream tail %q: want the server shutting down error event", rest)
	}
}
