package ragclient

import (
	"context"
	"errors"
	"iter"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"center-service/internal/config"
	"center-service/internal/domain"
	ragv1 "center-service/internal/gen/ragv1"
)

const testDeadline = 5 * time.Second

type item struct {
	msg Message
	err error
}

func collect(seq iter.Seq2[Message, error]) []item {
	var out []item
	for m, err := range seq {
		out = append(out, item{m, err})
	}
	return out
}

// assertErrorLast fails unless exactly one error was yielded and it was the last item.
func assertErrorLast(t *testing.T, items []item) error {
	t.Helper()
	if len(items) == 0 {
		t.Fatal("iterator yielded nothing: a failed ask must yield exactly one error")
	}
	for i, it := range items[:len(items)-1] {
		if it.err != nil {
			t.Fatalf("item %d is an error (%v) but is not last: an error must end the stream", i, it.err)
		}
	}
	last := items[len(items)-1].err
	if last == nil {
		t.Fatal("last item has no error: a failed ask must end with one error")
	}
	return last
}

func TestAskStreamPassesSourcesThenDeltas(t *testing.T) {
	fake := newFakeRAG()
	fake.msgs = []*ragv1.AskStreamResponse{
		sourcesMsg(
			&ragv1.Source{DocumentId: "hash-a", Filename: "refunds.pdf", Snippet: "Refunds within 30 days", SimilarityScore: 0.71},
			&ragv1.Source{DocumentId: "hash-b", Filename: "terms.md", Snippet: "Terms apply", SimilarityScore: 0.52},
		),
		deltaMsg("Refunds are "), deltaMsg("accepted within "), deltaMsg("30 days."),
	}
	c := startFake(t, fake, testDeadline)

	items := collect(c.AskStream(context.Background(), "What is the refund window?", []string{"hash-a", "hash-b"}))
	if len(items) != 4 {
		t.Fatalf("got %d items, want sources + 3 deltas: %+v", len(items), items)
	}
	for i, it := range items {
		if it.err != nil {
			t.Fatalf("item %d: unexpected error %v: a stream that ends cleanly yields no error", i, it.err)
		}
	}
	want := []domain.Source{
		{Hash: "hash-a", Filename: "refunds.pdf", Snippet: "Refunds within 30 days", Score: 0.71},
		{Hash: "hash-b", Filename: "terms.md", Snippet: "Terms apply", Score: 0.52},
	}
	got := items[0].msg
	if got.Delta != "" || len(got.Sources) != len(want) {
		t.Fatalf("message 1 = %+v, want only the two sources", got)
	}
	for i := range want {
		if got.Sources[i] != want[i] {
			t.Errorf("source %d = %+v, want %+v: every proto field must map", i, got.Sources[i], want[i])
		}
	}
	for i, d := range []string{"Refunds are ", "accepted within ", "30 days."} {
		if m := items[i+1].msg; m.Delta != d || m.Sources != nil {
			t.Errorf("message %d = %+v, want only delta %q", i+2, m, d)
		}
	}
}

func TestAskStreamSendsQueryAndHashes(t *testing.T) {
	tests := []struct {
		name   string
		hashes []string
	}{
		{"selection", []string{"h1", "h2"}},
		{"empty selection searches everything", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeRAG()
			fake.msgs = []*ragv1.AskStreamResponse{sourcesMsg()}
			c := startFake(t, fake, testDeadline)
			collect(c.AskStream(context.Background(), "q?", tc.hashes))

			req := fake.request()
			if req == nil {
				t.Fatal("server got no request")
			}
			if req.GetQuery() != "q?" {
				t.Errorf("query = %q, want the caller's question unchanged", req.GetQuery())
			}
			if len(req.GetDocumentIds()) != len(tc.hashes) {
				t.Fatalf("document_ids = %v, want %v", req.GetDocumentIds(), tc.hashes)
			}
			for i, h := range tc.hashes {
				if req.GetDocumentIds()[i] != h {
					t.Errorf("document_ids = %v, want %v", req.GetDocumentIds(), tc.hashes)
				}
			}
			if req.GetUserId() != "" || len(req.GetChatHistory()) != 0 {
				t.Errorf("user_id/chat_history = %q/%v, want empty (PRD L8, L9)", req.GetUserId(), req.GetChatHistory())
			}
		})
	}
}

// An empty sources message followed by no deltas is a valid, error-free answer (E8, E9).
func TestAskStreamEmptySourcesIsNotAnError(t *testing.T) {
	fake := newFakeRAG()
	fake.msgs = []*ragv1.AskStreamResponse{sourcesMsg()}
	c := startFake(t, fake, testDeadline)

	items := collect(c.AskStream(context.Background(), "q?", nil))
	if len(items) != 1 || items[0].err != nil {
		t.Fatalf("items = %+v, want one sources message and no error", items)
	}
	if items[0].msg.Sources == nil || len(items[0].msg.Sources) != 0 {
		t.Errorf("message 1 Sources = %#v, want a non-nil empty slice", items[0].msg.Sources)
	}
}

func TestAskStreamMapsStatusCodes(t *testing.T) {
	tests := []struct {
		code codes.Code
		want error // nil: neither sentinel
	}{
		{codes.InvalidArgument, domain.ErrInvalidQuery},
		{codes.Unavailable, domain.ErrRAGUnavailable},
		{codes.ResourceExhausted, domain.ErrRAGUnavailable},
		{codes.DeadlineExceeded, domain.ErrRAGUnavailable},
		{codes.Internal, nil},
	}
	for _, tc := range tests {
		t.Run(tc.code.String(), func(t *testing.T) {
			fake := newFakeRAG()
			fake.endErr = status.Error(tc.code, "server says no")
			c := startFake(t, fake, testDeadline)

			err := assertErrorLast(t, collect(c.AskStream(context.Background(), "q?", nil)))
			isInvalid := errors.Is(err, domain.ErrInvalidQuery)
			isUnavailable := errors.Is(err, domain.ErrRAGUnavailable)
			switch tc.want {
			case nil:
				if isInvalid || isUnavailable {
					t.Fatalf("%v mapped to a sentinel (%v): unknown codes must stay internal errors", tc.code, err)
				}
			default:
				if !errors.Is(err, tc.want) {
					t.Fatalf("%v gave %v, want %v", tc.code, err, tc.want)
				}
				if isInvalid && isUnavailable {
					t.Fatalf("%v matches both sentinels: %v", tc.code, err)
				}
			}
		})
	}
}

// E10: deltas already yielded stay, then one ErrRAGUnavailable ends the stream.
func TestAskStreamErrorAfterDeltas(t *testing.T) {
	fake := newFakeRAG()
	fake.msgs = []*ragv1.AskStreamResponse{sourcesMsg(), deltaMsg("partial "), deltaMsg("answer")}
	fake.endErr = status.Error(codes.Unavailable, "groq dropped")
	c := startFake(t, fake, testDeadline)

	items := collect(c.AskStream(context.Background(), "q?", nil))
	err := assertErrorLast(t, items)
	if len(items) != 4 {
		t.Fatalf("got %d items, want sources + 2 deltas + error", len(items))
	}
	if items[1].msg.Delta != "partial " || items[2].msg.Delta != "answer" {
		t.Errorf("deltas before the error = %q, %q", items[1].msg.Delta, items[2].msg.Delta)
	}
	if !errors.Is(err, domain.ErrRAGUnavailable) {
		t.Fatalf("error = %v, want ErrRAGUnavailable", err)
	}
}

// E3: stopping the loop cancels the RPC.
func TestAskStreamEarlyBreakCancelsRPC(t *testing.T) {
	fake := newFakeRAG()
	fake.msgs = []*ragv1.AskStreamResponse{sourcesMsg(), deltaMsg("one")}
	fake.block = true
	// A deadline far beyond waitCancelled's wait, so only the break itself can cancel the RPC.
	c := startFake(t, fake, time.Hour)

	for range c.AskStream(context.Background(), "q?", nil) {
		break
	}
	fake.waitCancelled(t, "the caller broke out of the loop")
}

// E4: the caller's own cancel is reported as context.Canceled, not as a service failure.
func TestAskStreamCallerCancel(t *testing.T) {
	fake := newFakeRAG()
	fake.msgs = []*ragv1.AskStreamResponse{sourcesMsg()}
	fake.block = true
	c := startFake(t, fake, testDeadline)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var items []item
	for m, err := range c.AskStream(ctx, "q?", nil) {
		items = append(items, item{m, err})
		if err == nil {
			cancel()
		}
	}
	err := assertErrorLast(t, items)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled so the handler can tell a gone client from a down service", err)
	}
	if errors.Is(err, domain.ErrRAGUnavailable) {
		t.Fatalf("caller cancel mapped to ErrRAGUnavailable: %v", err)
	}
	fake.waitCancelled(t, "the caller cancelled its context")
}

// E5: the ask deadline ends the stream as unavailable and cancels the RPC.
func TestAskStreamDeadline(t *testing.T) {
	fake := newFakeRAG()
	fake.msgs = []*ragv1.AskStreamResponse{sourcesMsg()}
	fake.block = true
	c := startFake(t, fake, 200*time.Millisecond)

	err := assertErrorLast(t, collect(c.AskStream(context.Background(), "q?", nil)))
	if !errors.Is(err, domain.ErrRAGUnavailable) {
		t.Fatalf("error = %v, want ErrRAGUnavailable once [go.ask].deadline passes", err)
	}
	fake.waitCancelled(t, "the deadline passed")
}

// E1: nothing listening yields ErrRAGUnavailable; New itself never dials.
func TestAskStreamUnreachable(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	lis.Close()

	var cfg config.Config
	cfg.RAGGRPC.Host = "127.0.0.1"
	cfg.RAGGRPC.Port = port
	cfg.Go.Ask.Deadline = config.Duration(testDeadline)
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New with nothing listening: %v (New must not dial or ping)", err)
	}
	defer c.Close()

	start := time.Now()
	err = assertErrorLast(t, collect(c.AskStream(context.Background(), "q?", nil)))
	if !errors.Is(err, domain.ErrRAGUnavailable) {
		t.Fatalf("error = %v, want ErrRAGUnavailable", err)
	}
	if took := time.Since(start); took >= testDeadline {
		t.Errorf("unreachable server took %v: it must fail fast, not wait for the deadline", took)
	}
}
