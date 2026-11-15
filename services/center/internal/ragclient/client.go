// Package ragclient is the Go client for the Python RAG service (rag.v1.RagEngineService).
// It is the only package that imports gRPC: callers get domain types and domain errors.
package ragclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"net"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"center-service/internal/config"
	"center-service/internal/domain"
	ragv1 "center-service/internal/gen/ragv1"
)

// Message is one item of an answer stream. The first message carries only Sources (a non-nil,
// possibly empty slice); every later message carries only a Delta of the answer text.
type Message struct {
	Sources []domain.Source
	Delta   string
}

type Client struct {
	conn     *grpc.ClientConn
	stub     ragv1.RagEngineServiceClient
	deadline time.Duration
}

// New builds a client for [rag_grpc].host:port. It does not dial: the connection is made on
// the first ask, so the center starts (and ingests) while rag-server is down. Plaintext is
// deliberate: compose binds the port to 127.0.0.1 and v1 has no auth (PRD L8).
func New(cfg config.Config) (*Client, error) {
	target := net.JoinHostPort(cfg.RAGGRPC.Host, strconv.Itoa(cfg.RAGGRPC.Port))
	return newClient(target, cfg.Go.Ask.Deadline.Std(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// newClient lets tests swap the transport (bufconn) through dial options.
func newClient(target string, deadline time.Duration, opts ...grpc.DialOption) (*Client, error) {
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("ragclient: new client for %s: %w", target, err)
	}
	return &Client{conn: conn, stub: ragv1.NewRagEngineServiceClient(conn), deadline: deadline}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// AskStream streams an answer to query, searching only the documents with the given content
// hashes (empty searches all). The whole stream runs under [go.ask].deadline. Stopping the
// loop, cancelling ctx or reaching the deadline cancels the RPC, so Python stops calling Groq.
// At most one error is yielded, always last: ErrInvalidQuery, ErrRAGUnavailable,
// context.Canceled when ctx was cancelled, or another error for anything unexpected.
func (c *Client) AskStream(ctx context.Context, query string, hashes []string) iter.Seq2[Message, error] {
	return func(yield func(Message, error) bool) {
		callCtx, cancel := context.WithTimeout(ctx, c.deadline)
		defer cancel()

		stream, err := c.stub.AskStream(callCtx, &ragv1.AskStreamRequest{Query: query, DocumentIds: hashes})
		if err != nil {
			yield(Message{}, mapError(ctx, err))
			return
		}
		first := true
		for {
			resp, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				yield(Message{}, mapError(ctx, err))
				return
			}
			var msg Message
			if first {
				msg.Sources = toSources(resp.GetContextUsed())
				first = false
			} else {
				msg.Delta = resp.GetAnswer()
			}
			if !yield(msg, nil) {
				return
			}
		}
	}
}

func toSources(in []*ragv1.Source) []domain.Source {
	out := make([]domain.Source, 0, len(in))
	for _, s := range in {
		out = append(out, domain.Source{
			Hash:     s.GetDocumentId(),
			Filename: s.GetFilename(),
			Snippet:  s.GetSnippet(),
			Score:    s.GetSimilarityScore(),
		})
	}
	return out
}

// mapError turns a gRPC failure into a domain error. The caller's own cancel is checked first:
// gRPC reports it as CANCELED, and the handler must tell "client left" from "service down".
func mapError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return fmt.Errorf("ragclient: ask stream: %w", context.Canceled)
	}
	st := status.Convert(err)
	switch st.Code() {
	case codes.InvalidArgument:
		return fmt.Errorf("%w: %s", domain.ErrInvalidQuery, st.Message())
	// RESOURCE_EXHAUSTED is the Python worker pool being full; DEADLINE_EXCEEDED is
	// [go.ask].deadline; UNAVAILABLE covers connection failures and Groq, Ollama or Qdrant down.
	case codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded:
		return fmt.Errorf("%w: %s: %s", domain.ErrRAGUnavailable, st.Code(), st.Message())
	}
	return fmt.Errorf("ragclient: ask stream: %s: %s", st.Code(), st.Message())
}
