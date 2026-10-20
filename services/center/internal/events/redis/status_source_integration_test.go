//go:build integration

package redis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"center-service/internal/config"
	"center-service/internal/events"
	"center-service/internal/status"
)

var _ status.Source = (*StatusSource)(nil)

// These tests need a real Redis >= 7.2 (XINFO CONSUMERS idle semantics): `make up` or
// `docker run -p 6379:6379 redis:7.4`. Point REDIS_ADDR elsewhere to use another instance.
func newSource(t *testing.T) (*StatusSource, *Client, *bytes.Buffer) {
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

	stream := "test.status." + uuid.NewString()
	ctx := context.Background()
	client, err := NewClient(ctx, config.RedisConfig{
		Host: host, Port: port, Stream: "unused", StreamMaxLen: 1000,
		StatusStream: stream, StatusConsumerGroup: "center-status",
	})
	if err != nil {
		t.Skipf("redis not reachable at %s: %v", addr, err)
	}
	t.Cleanup(func() {
		client.rdb.Del(context.Background(), stream)
		client.Close()
	})

	logs := &bytes.Buffer{}
	return NewStatusSource(client, slog.New(slog.NewTextHandler(logs, nil))), client, logs
}

func add(t *testing.T, c *Client, payload string) string {
	t.Helper()
	id, err := c.rdb.XAdd(context.Background(), &goredis.XAddArgs{
		Stream: c.StatusStream, Values: map[string]any{"event_type": "IngestionStatus", "payload": payload},
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestEnsureGroupIsIdempotentAndStartsAtTheBeginning(t *testing.T) {
	src, client, _ := newSource(t)
	ctx := context.Background()

	// An entry published before the group exists must still be delivered: the group starts at 0.
	first := add(t, client, "before-group")
	if err := src.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := src.EnsureGroup(ctx); err != nil { // BUSYGROUP is not an error
		t.Fatalf("second EnsureGroup: %v", err)
	}
	got, err := src.ReadNew(ctx, "c1", 200*time.Millisecond, 10)
	if err != nil || len(got) != 1 || got[0].ID != first {
		t.Fatalf("entries=%v err=%v, want the pre-existing entry %s", got, err, first)
	}
}

func TestEnsureGroupCreatesMissingStream(t *testing.T) {
	src, client, _ := newSource(t)
	if err := src.EnsureGroup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n, err := client.rdb.Exists(context.Background(), client.StatusStream).Result(); err != nil || n != 1 {
		t.Fatalf("stream exists=%d err=%v, want MKSTREAM to have created it", n, err)
	}
}

func TestReadNewTimesOutWithoutError(t *testing.T) {
	src, _, _ := newSource(t)
	ctx := context.Background()
	if err := src.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	got, err := src.ReadNew(ctx, "c1", 150*time.Millisecond, 10)
	if err != nil || len(got) != 0 {
		t.Fatalf("entries=%v err=%v, want none and no error", got, err)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatal("returned before the block time")
	}
}

// ReadNew reads ">" only: an entry that is delivered but not acked is not handed out again,
// it comes back only through ClaimStale.
func TestReadNewNeverRedeliversPendingEntries(t *testing.T) {
	src, client, _ := newSource(t)
	ctx := context.Background()
	add(t, client, "p1")
	if err := src.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := src.ReadNew(ctx, "c1", 100*time.Millisecond, 10); len(got) != 1 {
		t.Fatalf("first read got %d entries", len(got))
	}
	if got, _ := src.ReadNew(ctx, "c1", 100*time.Millisecond, 10); len(got) != 0 {
		t.Fatalf("second read got %d entries, want 0 (pending is not redelivered)", len(got))
	}
}

func TestClaimStaleTakesOverIdleEntriesAndCountsDeliveries(t *testing.T) {
	src, client, _ := newSource(t)
	ctx := context.Background()
	id := add(t, client, "stuck")
	if err := src.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := src.ReadNew(ctx, "dead", 100*time.Millisecond, 10); len(got) != 1 {
		t.Fatal("setup: read failed")
	}
	if n, err := src.DeliveryCount(ctx, id); err != nil || n != 1 {
		t.Fatalf("delivery count after first read = %d, err %v, want 1", n, err)
	}

	// Not idle long enough: nothing is claimed.
	if got, err := src.ClaimStale(ctx, "me", time.Hour, 10); err != nil || len(got) != 0 {
		t.Fatalf("claimed %v (err %v) before min_idle", got, err)
	}

	time.Sleep(150 * time.Millisecond)
	got, err := src.ClaimStale(ctx, "me", 100*time.Millisecond, 10)
	if err != nil || len(got) != 1 {
		t.Fatalf("claimed %v err %v, want the stuck entry", got, err)
	}
	if got[0].ID != id || got[0].Values["payload"] != "stuck" || got[0].Values["event_type"] != "IngestionStatus" {
		t.Fatalf("claimed entry lost its fields: %+v", got[0])
	}
	// Without JUSTID the claim increments the delivery count; the poison check depends on it.
	if n, err := src.DeliveryCount(ctx, id); err != nil || n != 2 {
		t.Fatalf("delivery count after claim = %d, err %v, want 2", n, err)
	}
	owner, err := client.rdb.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: client.StatusStream, Group: client.StatusGroup, Start: "-", End: "+", Count: 10,
	}).Result()
	if err != nil || len(owner) != 1 || owner[0].Consumer != "me" {
		t.Fatalf("pending owner = %+v err %v, want me", owner, err)
	}
}

func TestClaimStaleLogsEntriesDeletedFromTheStream(t *testing.T) {
	src, client, logs := newSource(t)
	ctx := context.Background()
	id := add(t, client, "will-be-trimmed")
	if err := src.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := src.ReadNew(ctx, "dead", 100*time.Millisecond, 10); len(got) != 1 {
		t.Fatal("setup: read failed")
	}
	if err := client.rdb.XDel(ctx, client.StatusStream, id).Err(); err != nil { // as stream trimming would
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)

	got, err := src.ClaimStale(ctx, "me", 100*time.Millisecond, 10)
	if err != nil || len(got) != 0 {
		t.Fatalf("claimed %v err %v, want nothing for a deleted entry", got, err)
	}
	if !strings.Contains(logs.String(), id) {
		t.Fatalf("deleted entry %s not logged; logs: %s", id, logs)
	}
}

func TestAckRemovesTheEntryFromPending(t *testing.T) {
	src, client, _ := newSource(t)
	ctx := context.Background()
	id := add(t, client, "done")
	if err := src.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	src.ReadNew(ctx, "c1", 100*time.Millisecond, 10)
	if err := src.Ack(ctx, id); err != nil {
		t.Fatal(err)
	}
	if n, err := src.DeliveryCount(ctx, id); err != nil || n != 0 {
		t.Fatalf("delivery count after ack = %d, err %v, want 0", n, err)
	}
}

func TestDeliveryCountOfUnknownEntryIsZero(t *testing.T) {
	src, _, _ := newSource(t)
	if err := src.EnsureGroup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n, err := src.DeliveryCount(context.Background(), "1-1"); err != nil || n != 0 {
		t.Fatalf("n=%d err=%v, want 0/nil", n, err)
	}
}

func TestCleanupConsumersDeletesOnlyIdleConsumersWithoutPending(t *testing.T) {
	src, client, _ := newSource(t)
	ctx := context.Background()
	add(t, client, "held")
	if err := src.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	mk := func(name string) {
		if err := client.rdb.XGroupCreateConsumer(ctx, client.StatusStream, client.StatusGroup, name).Err(); err != nil {
			t.Fatal(err)
		}
	}
	mk("idle-empty")
	mk("me")
	if got, _ := src.ReadNew(ctx, "busy", 100*time.Millisecond, 1); len(got) != 1 { // busy now holds 1 pending
		t.Fatal("setup: busy consumer read nothing")
	}
	time.Sleep(250 * time.Millisecond)
	mk("fresh") // created just now: below the idle threshold

	deleted, err := src.CleanupConsumers(ctx, "me", 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(deleted) != "[idle-empty]" {
		t.Fatalf("deleted = %v, want only idle-empty (not me, not busy with pending, not fresh)", deleted)
	}

	consumers, err := client.rdb.XInfoConsumers(ctx, client.StatusStream, client.StatusGroup).Result()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range consumers {
		names = append(names, c.Name)
	}
	for _, want := range []string{"me", "busy", "fresh"} {
		if !strings.Contains(strings.Join(names, ","), want) {
			t.Errorf("consumer %s was deleted; remaining %v", want, names)
		}
	}
	// The pending entry of the busy consumer must survive.
	pending, err := client.rdb.XPending(ctx, client.StatusStream, client.StatusGroup).Result()
	if err != nil || pending.Count != 1 {
		t.Fatalf("pending = %+v err %v, want the busy consumer's entry intact", pending, err)
	}
}

func TestReadNewAndClaimStaleReportAMissingGroup(t *testing.T) {
	src, client, _ := newSource(t)
	ctx := context.Background()
	if err := src.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.rdb.Del(ctx, client.StatusStream).Err(); err != nil { // what a Redis restart without the group does
		t.Fatal(err)
	}

	if _, err := src.ReadNew(ctx, "c1", 50*time.Millisecond, 10); !errors.Is(err, events.ErrNoGroup) {
		t.Errorf("ReadNew error = %v, want events.ErrNoGroup", err)
	}
	if _, err := src.ClaimStale(ctx, "c1", time.Second, 10); !errors.Is(err, events.ErrNoGroup) {
		t.Errorf("ClaimStale error = %v, want events.ErrNoGroup", err)
	}
}

// The whole point of ErrNoGroup: a running worker keeps handling entries after the group (and
// the stream) vanish underneath it.
func TestWorkerKeepsWorkingAfterTheGroupIsDeleted(t *testing.T) {
	src, client, _ := newSource(t)
	var mu sync.Mutex
	var seen []string
	handler := handlerFunc(func(_ context.Context, e events.StreamEntry) bool {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, e.Values["payload"].(string))
		return true
	})
	worker := status.NewWorker("host/status/it", status.WorkerConfig{
		MinIdle: time.Minute, RecoveryInterval: 300 * time.Millisecond, PoisonThreshold: 3,
		CleanupIdle: time.Hour, ReadBlock: 100 * time.Millisecond, BatchSize: 10, RetryDelay: 50 * time.Millisecond,
	}, src, handler, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { worker.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	handled := func(payload string) bool {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			for _, p := range seen {
				if p == payload {
					mu.Unlock()
					return true
				}
			}
			mu.Unlock()
			time.Sleep(25 * time.Millisecond)
		}
		return false
	}

	add(t, client, "before")
	if !handled("before") {
		t.Fatal("setup: the first entry was not handled")
	}
	if err := client.rdb.Del(context.Background(), client.StatusStream).Err(); err != nil {
		t.Fatal(err)
	}
	add(t, client, "after")
	if !handled("after") {
		t.Fatal("the worker never recovered from the deleted group: the entry added afterwards was not handled")
	}
}

type handlerFunc func(ctx context.Context, e events.StreamEntry) bool

func (h handlerFunc) Handle(ctx context.Context, e events.StreamEntry) bool { return h(ctx, e) }
