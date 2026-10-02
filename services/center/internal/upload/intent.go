package upload

import (
	"time"
)

type State int

const (
	StateUnknown State = iota
	StateWaiting
	StateUploaded
	StateHashing
	StateHashed
	StateExpired
	StateFailed
)

var stateName = map[State]string{
	StateUnknown:  "UNKNOWN",
	StateWaiting:  "WAITING",
	StateUploaded: "UPLOADED",
	StateHashing:  "HASHING",
	StateHashed:   "HASHED",
	StateExpired:  "EXPIRED",
	StateFailed:   "FAILED",
}

func (s State) String() string {
	return stateName[s]
}

// StateFromString maps a stored status back to its State, or StateUnknown.
func StateFromString(name string) State {
	for state, n := range stateName {
		if n == name {
			return state
		}
	}
	return StateUnknown
}

type Intent struct {
	ID             string
	FileName       string
	TempBucketPath *string
	CreatedAt      time.Time
	ExpiresAt      time.Time
	CompletedAt    *time.Time
	Status         State
}
