package sqlite_test

import (
	"context"
	"testing"
	"time"

	"center-service/internal/domain"
	"center-service/internal/store/sqlite"
)

func fixedClock(t time.Time) func() time.Time { return func() time.Time { return t } }

func TestClaimOutboxMessageStampsClaimedAtFromTheClock(t *testing.T) {
	s, db := newStore(t)
	sqlite.SetNow(s, fixedClock(now))
	insertDoc(t, db, hash, domain.DocumentProcessing, 0)
	res, err := db.Exec(`INSERT INTO outbox (hash, status, created_at) VALUES (?,?,?)`, hash, domain.OutboxPending, created)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()

	won, err := s.ClaimOutboxMessage(context.Background(), "w1", id)
	if err != nil || !won {
		t.Fatalf("claim = %v, %v; want a win on a PENDING row", won, err)
	}
	var claimedAt time.Time
	if err := db.QueryRow(`SELECT claimed_at FROM outbox WHERE id = ?`, id).Scan(&claimedAt); err != nil {
		t.Fatal(err)
	}
	if !claimedAt.Equal(now) {
		t.Errorf("claimed_at = %v, want %v: the claim must use the store clock so recovery cutoffs are testable", claimedAt, now)
	}
}
