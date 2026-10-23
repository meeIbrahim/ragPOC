package events

import "errors"

// StreamEntry is one entry read from a Redis stream. It keeps the stream packages free of the
// Redis client types.
type StreamEntry struct {
	ID     string
	Values map[string]any
}

// ErrNoGroup means the stream's consumer group no longer exists (Redis restarted without the
// group in its snapshot, the stream was deleted, FLUSHALL). The reader must create it again.
var ErrNoGroup = errors.New("consumer group does not exist")
