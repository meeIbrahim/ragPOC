package ragclient

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	ragv1 "center-service/internal/gen/ragv1"
)

// fakeRAG is a scripted RagEngineService. It sends msgs in order, then either returns endErr
// or, with block set, waits until its stream context ends. It records the request and closes
// streamDone when the stream context ends, so tests can prove the RPC was cancelled.
type fakeRAG struct {
	ragv1.UnimplementedRagEngineServiceServer

	msgs   []*ragv1.AskStreamResponse
	endErr error
	block  bool

	mu         sync.Mutex
	req        *ragv1.AskStreamRequest
	streamDone chan struct{}
}

func newFakeRAG() *fakeRAG {
	return &fakeRAG{streamDone: make(chan struct{})}
}

func (f *fakeRAG) AskStream(req *ragv1.AskStreamRequest, stream grpc.ServerStreamingServer[ragv1.AskStreamResponse]) error {
	f.mu.Lock()
	f.req = req
	f.mu.Unlock()
	go func() {
		<-stream.Context().Done()
		close(f.streamDone)
	}()
	for _, m := range f.msgs {
		if err := stream.Send(m); err != nil {
			return err
		}
	}
	if f.block {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	return f.endErr
}

func (f *fakeRAG) request() *ragv1.AskStreamRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.req
}

// waitCancelled fails the test unless the server saw its stream context end.
func (f *fakeRAG) waitCancelled(t *testing.T, why string) {
	t.Helper()
	select {
	case <-f.streamDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("server stream context still live after %s: the RPC must be cancelled so Python stops calling Groq", why)
	}
}

// startFake serves fake over an in-memory bufconn listener and returns a Client wired to it.
func startFake(t *testing.T, fake *fakeRAG, deadline time.Duration) *Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	ragv1.RegisterRagEngineServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	c, err := newClient("passthrough:///bufnet", deadline,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func sourcesMsg(srcs ...*ragv1.Source) *ragv1.AskStreamResponse {
	return &ragv1.AskStreamResponse{ContextUsed: srcs}
}

func deltaMsg(text string) *ragv1.AskStreamResponse {
	return &ragv1.AskStreamResponse{Answer: text}
}
