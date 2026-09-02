package outbox

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"ragingest/internal/db"
)

type countingPublisher struct {
	calls int32
	err   error
}

func (p *countingPublisher) Publish(ctx context.Context, hashID, objectPath, contentType string) error {
	atomic.AddInt32(&p.calls, 1)
	return p.err
}

func TestSweepPublishesUnpublishedRowsAndMarksThem(t *testing.T) {
	conn, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer conn.Close()
	if err := db.Insert(conn, "abc123", "objects/abc.pdf"); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	pub := &countingPublisher{}
	sweep(context.Background(), conn, pub)

	if atomic.LoadInt32(&pub.calls) != 1 {
		t.Fatalf("publish calls = %d, want 1", pub.calls)
	}
	doc, err := db.GetByHash(conn, "abc123")
	if err != nil || doc == nil || !doc.PublishedAt.Valid {
		t.Fatalf("expected doc to be marked published, got %+v (err=%v)", doc, err)
	}
}

func TestSweepLeavesRowUnpublishedOnPublishError(t *testing.T) {
	conn, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer conn.Close()
	if err := db.Insert(conn, "abc123", "objects/abc.pdf"); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	pub := &countingPublisher{err: context.DeadlineExceeded}
	sweep(context.Background(), conn, pub)

	doc, err := db.GetByHash(conn, "abc123")
	if err != nil || doc == nil {
		t.Fatalf("GetByHash: doc=%+v err=%v", doc, err)
	}
	if doc.PublishedAt.Valid {
		t.Fatalf("expected doc to stay unpublished after publish error, got %+v", doc.PublishedAt)
	}
}

func TestStartRunsSweepOnEachTick(t *testing.T) {
	conn, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer conn.Close()
	if err := db.Insert(conn, "abc123", "objects/abc.pdf"); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	pub := &countingPublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	Start(ctx, conn, pub, 10*time.Millisecond)
	defer cancel()

	deadline := time.After(500 * time.Millisecond)
	for {
		if atomic.LoadInt32(&pub.calls) >= 1 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("Start did not publish the pending row within 500ms")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
