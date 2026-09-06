package ingestion

import "time"

type State int

const (
	StateUnknown State = iota
	StateIdle
	StateProcessing
	StateComplete
	StateError
)

var stateName = map[State]string{
	StateUnknown:    "unknown",
	StateIdle:       "idle",
	StateProcessing: "processing",
	StateComplete:   "complete",
	StateError:      "error",
}

func (s State) String() string {
	return stateName[s]
}

type Process struct {
	ContentHash string
	StartedAt   time.Time
	CompletedAt *time.Time
	State       State
}
