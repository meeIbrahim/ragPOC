package domain

import "time"

type OutboxStatus string

const (
	OutboxPending   OutboxStatus = "PENDING"
	OutboxClaimed   OutboxStatus = "CLAIMED"
	OutboxPublished OutboxStatus = "PUBLISHED"
	OutboxBlocked   OutboxStatus = "BLOCKED"
)

type OutboxEvent struct {
	Id        int
	Hash      string
	Status    OutboxStatus
	ClaimedBy *string
	ClaimedAt *time.Time
	Retries   int
	CreatedAt *time.Time
}
