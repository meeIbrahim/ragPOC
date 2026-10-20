package status

import (
	"context"
	"time"

	"center-service/internal/events"
)

// Source is the status stream as the worker sees it. The Redis implementation lives in
// internal/events/redis.
type Source interface {
	// EnsureGroup creates the consumer group at the start of the stream (creating the stream
	// if needed). An existing group is not an error.
	EnsureGroup(ctx context.Context) error
	// ReadNew reads entries never delivered to any consumer of the group, waiting up to
	// block. It returns no entries and no error when the wait times out.
	ReadNew(ctx context.Context, consumer string, block time.Duration, count int64) ([]events.StreamEntry, error)
	// ClaimStale takes over up to count entries that have been pending longer than minIdle.
	// Claiming increments each entry's delivery count.
	ClaimStale(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]events.StreamEntry, error)
	// DeliveryCount is how many times the entry has been delivered; 0 when it is not pending.
	DeliveryCount(ctx context.Context, id string) (int64, error)
	Ack(ctx context.Context, id string) error
	// CleanupConsumers deletes consumers other than self that have no pending entries and
	// have been idle longer than idle, and returns their names.
	CleanupConsumers(ctx context.Context, self string, idle time.Duration) (deleted []string, err error)
}
