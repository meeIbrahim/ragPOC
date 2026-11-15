//go:build integration

package ragclient

import (
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"center-service/internal/config"
	"center-service/internal/domain"
)

// Real wire to a running Python rag.server (`make run_server`; any GROQ_API_KEY value works).
// Python rejects an empty query before retrieval or any Groq call, so this needs no Groq.
func TestAskStreamRealServerRejectsEmptyQuery(t *testing.T) {
	addr := os.Getenv("RAG_GRPC_ADDR")
	if addr == "" {
		t.Skip("RAG_GRPC_ADDR not set; start rag.server and set it, e.g. localhost:50051")
	}
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("RAG_GRPC_ADDR=%q: %v", addr, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("RAG_GRPC_ADDR=%q: %v", addr, err)
	}
	var cfg config.Config
	cfg.RAGGRPC.Host = host
	cfg.RAGGRPC.Port = port
	cfg.Go.Ask.Deadline = config.Duration(30 * time.Second)
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var items []item
	for m, err := range c.AskStream(context.Background(), "", nil) {
		items = append(items, item{m, err})
	}
	err = assertErrorLast(t, items)
	if errors.Is(err, domain.ErrRAGUnavailable) {
		t.Skipf("rag.server not reachable at %s: %v", addr, err)
	}
	if !errors.Is(err, domain.ErrInvalidQuery) {
		t.Fatalf("empty query gave %v, want ErrInvalidQuery from the real server", err)
	}
	if len(items) != 1 {
		t.Fatalf("got %d items, want only the error: a rejected query streams nothing", len(items))
	}
}
