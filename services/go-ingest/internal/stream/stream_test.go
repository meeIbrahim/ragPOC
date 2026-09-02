package stream

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestPublishAddsEntryToStream(t *testing.T) {
	mr := miniredis.RunT(t)
	publisher := NewRedisPublisher(mr.Addr(), "ingest.docs")

	if err := publisher.Publish(context.Background(), "abc123", "objects/abc.pdf", "application/pdf"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	entries, err := client.XRange(context.Background(), "ingest.docs", "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d stream entries, want 1", len(entries))
	}
	if entries[0].Values["hash_id"] != "abc123" {
		t.Errorf("hash_id = %v, want abc123", entries[0].Values["hash_id"])
	}
	if entries[0].Values["object_path"] != "objects/abc.pdf" {
		t.Errorf("object_path = %v, want objects/abc.pdf", entries[0].Values["object_path"])
	}
}
