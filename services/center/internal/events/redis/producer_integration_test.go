//go:build integration

package redis

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"center-service/internal/config"
	"center-service/internal/events"
)

// newProducerClient connects to the test Redis (see newSource) with its own ingest stream.
func newProducerClient(t *testing.T, maxLen int64) *Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(portText)
	stream := "test.ingest." + uuid.NewString()
	client, err := NewClient(context.Background(), config.RedisConfig{
		Host: host, Port: port, Stream: stream, StreamMaxLen: maxLen,
		StatusStream: "unused", StatusConsumerGroup: "unused",
	})
	if err != nil {
		t.Skipf("redis not reachable at %s: %v", addr, err)
	}
	t.Cleanup(func() {
		client.rdb.Del(context.Background(), stream)
		client.Close()
	})
	return client
}

func sampleEvent() *events.StreamIngestionEvent {
	return &events.StreamIngestionEvent{
		ID: uuid.NewString(), Hash: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
		Filename: "a.pdf", BucketPath: "ba/78/ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
		Size: 42, Attempts: 1, CreatedAt: time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC), SchemaVersion: 1,
	}
}

// The Python consumer reads event_type and a JSON payload from the configured stream (PRD 4.1).
func TestPublishWritesTheEventToTheConfiguredStream(t *testing.T) {
	client := newProducerClient(t, 1000)
	event := sampleEvent()

	id, err := NewProducer(client).PublishIngestionEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}

	entries, err := client.rdb.XRange(context.Background(), client.IngestionStream, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].ID != id {
		t.Fatalf("stream entries = %+v; want exactly the returned id %s", entries, id)
	}
	if entries[0].Values["event_type"] != "IngestionRequest" {
		t.Errorf("event_type = %v; want IngestionRequest (the consumer dispatches on it)", entries[0].Values["event_type"])
	}
	var got events.StreamIngestionEvent
	if err := json.Unmarshal([]byte(entries[0].Values["payload"].(string)), &got); err != nil {
		t.Fatalf("payload is not JSON: %v", err)
	}
	if got != *event {
		t.Errorf("payload = %+v; want the event round-tripped unchanged %+v", got, *event)
	}
}

// XADD trims with the configured MAXLEN, so a consumer outage cannot grow the stream unbounded.
// Trimming is approximate (whole macro nodes), so only an upper bound is asserted.
func TestPublishTrimsTheStreamToStreamMaxLen(t *testing.T) {
	client := newProducerClient(t, 10)
	producer := NewProducer(client)
	for range 500 {
		if _, err := producer.PublishIngestionEvent(context.Background(), sampleEvent()); err != nil {
			t.Fatal(err)
		}
	}
	n, err := client.rdb.XLen(context.Background(), client.IngestionStream).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n >= 500 || n < 10 {
		t.Errorf("stream length = %d after 500 publishes with stream_max_len=10; want it trimmed (approximately) to at least 10", n)
	}
}

func TestPublishWrapsTheRedisError(t *testing.T) {
	client := newProducerClient(t, 1000)
	client.rdb.Close()

	_, err := NewProducer(client).PublishIngestionEvent(context.Background(), sampleEvent())
	if !errors.Is(err, goredis.ErrClosed) || !strings.Contains(err.Error(), "publish ingestion event") {
		t.Errorf("err = %v; want the Redis error wrapped with what failed", err)
	}
}
