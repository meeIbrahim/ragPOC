package upload

import "time"

type State int

const (
	StateUnknown State = iota
	StateWaiting
	StateUploaded
	StateProcessing
	StateCompleted
	StateDuplicate
	StateFailed
	StateExpired
	StateVerificationFailed
)

var stateName = map[State]string{
	StateUnknown:            "unknown",
	StateWaiting:            "waiting",
	StateUploaded:           "uploaded",
	StateProcessing:         "processing",
	StateCompleted:          "completed",
	StateDuplicate:          "duplicate",
	StateFailed:             "failed",
	StateExpired:            "expired",
	StateVerificationFailed: "verification_failed",
}

func (s State) String() string {
	return stateName[s]
}

type Intent struct {
	ID          string
	FileName    string
	ObjectPath  string
	CreatedAt   time.Time
	ExpiresAt   time.Time
	CompletedAt *time.Time
	Status      State
}
