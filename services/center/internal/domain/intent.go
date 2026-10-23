package domain

import (
	"time"
)

type IntentState int

const (
	IntentUnknown IntentState = iota
	IntentWaiting
	IntentUploaded
	IntentHashing
	IntentHashed
	IntentExpired
	IntentFailed
)

var stateName = map[IntentState]string{
	IntentUnknown:  "UNKNOWN",
	IntentWaiting:  "WAITING",
	IntentUploaded: "UPLOADED",
	IntentHashing:  "HASHING",
	IntentHashed:   "HASHED",
	IntentExpired:  "EXPIRED",
	IntentFailed:   "FAILED",
}

func (s IntentState) String() string {
	return stateName[s]
}

// StateFromString maps a stored status back to its State, or StateUnknown.
func StateFromString(name string) IntentState {
	for state, n := range stateName {
		if n == name {
			return state
		}
	}
	return IntentUnknown
}

type Intent struct {
	ID             string
	FileName       string
	TempBucketPath *string
	Hash           *string
	CreatedAt      time.Time
	ExpiresAt      time.Time
	UploadedAt     *time.Time
	Status         IntentState
	ClaimedBy      *string
	ClaimedAt      *time.Time
	Retries        int
}
