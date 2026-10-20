package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"center-service/internal/events"
)

// StatusSource reads the ingest status stream through the center's consumer group.
type StatusSource struct {
	client *Client
	logger *slog.Logger
}

func NewStatusSource(client *Client, logger *slog.Logger) *StatusSource {
	return &StatusSource{client: client, logger: logger.With("component", "status-source")}
}

// EnsureGroup creates the group at ID 0, so entries published before the group existed are
// still delivered, and creates the stream when it does not exist yet.
func (s *StatusSource) EnsureGroup(ctx context.Context) error {
	err := s.client.rdb.XGroupCreateMkStream(ctx, s.client.StatusStream, s.client.StatusGroup, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create group %s on %s: %w", s.client.StatusGroup, s.client.StatusStream, err)
	}
	return nil
}

// ReadNew reads with ">" only, so it never re-reads entries already delivered to some consumer.
// Those come back through ClaimStale.
func (s *StatusSource) ReadNew(ctx context.Context, consumer string, block time.Duration, count int64) ([]events.StreamEntry, error) {
	streams, err := s.client.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    s.client.StatusGroup,
		Consumer: consumer,
		Streams:  []string{s.client.StatusStream, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) { // the block time elapsed with nothing to read
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read group: %w", err)
	}
	var entries []events.StreamEntry
	for _, stream := range streams {
		entries = append(entries, toEntries(stream.Messages)...)
	}
	return entries, nil
}

// ClaimStale uses XAUTOCLAIM without JUSTID: the delivery count must increment (the poison
// check reads it) and the payload is needed to process the entry. Entries that were deleted
// from the stream while pending (for example trimmed) are removed from the pending list by
// Redis and reported here; they are logged because such an event is lost.
func (s *StatusSource) ClaimStale(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]events.StreamEntry, error) {
	msgs, _, deleted, err := s.client.rdb.XAutoClaimWithDeleted(ctx, &redis.XAutoClaimArgs{
		Stream:   s.client.StatusStream,
		Group:    s.client.StatusGroup,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    count,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("autoclaim: %w", err)
	}
	if len(deleted) > 0 {
		s.logger.Error("pending status entries no longer exist in the stream; those events are lost", "ids", deleted)
	}
	return toEntries(msgs), nil
}

func (s *StatusSource) DeliveryCount(ctx context.Context, id string) (int64, error) {
	pending, err := s.client.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: s.client.StatusStream,
		Group:  s.client.StatusGroup,
		Start:  id,
		End:    id,
		Count:  1,
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("pending entry %s: %w", id, err)
	}
	if len(pending) == 0 {
		return 0, nil
	}
	return pending[0].RetryCount, nil
}

func (s *StatusSource) Ack(ctx context.Context, id string) error {
	if err := s.client.rdb.XAck(ctx, s.client.StatusStream, s.client.StatusGroup, id).Err(); err != nil {
		return fmt.Errorf("ack %s: %w", id, err)
	}
	return nil
}

// CleanupConsumers deletes consumers left behind by earlier process runs (every start uses a
// new name). A consumer with pending entries is never deleted: Redis would drop those entries
// silently. The idle time is the Redis 7.2+ "time since last attempted interaction".
func (s *StatusSource) CleanupConsumers(ctx context.Context, self string, idle time.Duration) ([]string, error) {
	consumers, err := s.client.rdb.XInfoConsumers(ctx, s.client.StatusStream, s.client.StatusGroup).Result()
	if err != nil {
		return nil, fmt.Errorf("list consumers: %w", err)
	}
	var deleted []string
	for _, c := range consumers {
		if c.Name == self || c.Pending != 0 || c.Idle <= idle {
			continue
		}
		if err := s.client.rdb.XGroupDelConsumer(ctx, s.client.StatusStream, s.client.StatusGroup, c.Name).Err(); err != nil {
			return deleted, fmt.Errorf("delete consumer %s: %w", c.Name, err)
		}
		deleted = append(deleted, c.Name)
	}
	return deleted, nil
}

func toEntries(msgs []redis.XMessage) []events.StreamEntry {
	entries := make([]events.StreamEntry, len(msgs))
	for i, m := range msgs {
		entries[i] = events.StreamEntry{ID: m.ID, Values: m.Values}
	}
	return entries
}
