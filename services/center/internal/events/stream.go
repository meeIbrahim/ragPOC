package events

// StreamEntry is one entry read from a Redis stream. It keeps the stream packages free of the
// Redis client types.
type StreamEntry struct {
	ID     string
	Values map[string]any
}
