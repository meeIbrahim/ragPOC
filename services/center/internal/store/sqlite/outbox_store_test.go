package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
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

type outboxSeed struct {
	status    domain.OutboxStatus
	claimedBy string // empty: no claim
	claimedAt time.Time
	retries   int
	createdAt time.Time
}

// seedOutbox inserts an outbox row for hash (seeding its document when needed) and returns its id.
func seedOutbox(t *testing.T, db *sql.DB, o outboxSeed) int64 {
	t.Helper()
	if _, err := db.Exec(`INSERT OR IGNORE INTO document_store (hash, perm_bucket_path, file_name, size, status, retries, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?)`, hash, "ba/78/"+hash, "a.txt", 3, domain.DocumentProcessing, 0, created, created); err != nil {
		t.Fatal(err)
	}
	if o.createdAt.IsZero() {
		o.createdAt = created
	}
	var claimedBy, claimedAt any
	if o.claimedBy != "" {
		claimedBy, claimedAt = o.claimedBy, o.claimedAt
	}
	res, err := db.Exec(`INSERT INTO outbox (hash, status, claimed_by, claimed_at, retries, created_at) VALUES (?,?,?,?,?,?)`,
		hash, o.status, claimedBy, claimedAt, o.retries, o.createdAt)
	if err != nil {
		t.Fatalf("seed outbox: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type outboxRow struct {
	status    string
	retries   int
	claimedBy sql.NullString
	claimedAt sql.NullTime
}

func readOutbox(t *testing.T, db *sql.DB, id int64) outboxRow {
	t.Helper()
	var r outboxRow
	if err := db.QueryRow(`SELECT status, retries, claimed_by, claimed_at FROM outbox WHERE id = ?`, id).
		Scan(&r.status, &r.retries, &r.claimedBy, &r.claimedAt); err != nil {
		t.Fatalf("read outbox %d: %v", id, err)
	}
	return r
}

// ---- FetchCandidates

func TestFetchCandidatesReturnsOnlyPendingOldestFirst(t *testing.T) {
	s, db := newStore(t)
	newest := seedOutbox(t, db, outboxSeed{status: domain.OutboxPending, createdAt: created.Add(3 * time.Minute)})
	oldest := seedOutbox(t, db, outboxSeed{status: domain.OutboxPending, createdAt: created.Add(1 * time.Minute)})
	middle := seedOutbox(t, db, outboxSeed{status: domain.OutboxPending, createdAt: created.Add(2 * time.Minute)})
	seedOutbox(t, db, outboxSeed{status: domain.OutboxClaimed, claimedBy: "w", claimedAt: now})
	seedOutbox(t, db, outboxSeed{status: domain.OutboxPublished})
	seedOutbox(t, db, outboxSeed{status: domain.OutboxBlocked})

	got, err := s.FetchCandidates(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for _, m := range got {
		ids = append(ids, m.Id)
	}
	if fmt.Sprint(ids) != fmt.Sprint([]int64{oldest, middle}) {
		t.Errorf("candidates = %v; want %v: only PENDING, oldest created_at first, at most limit (newest is %d)", ids, []int64{oldest, middle}, newest)
	}
}

// ---- ClaimOutboxMessage

func TestClaimOutboxMessageHasExactlyOneWinner(t *testing.T) {
	s, db := newClockedStore(t)
	id := seedOutbox(t, db, outboxSeed{status: domain.OutboxPending})

	const claimers = 8
	var wg sync.WaitGroup
	won := make(chan string, claimers)
	for i := range claimers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker := fmt.Sprintf("w%d", i)
			ok, err := s.ClaimOutboxMessage(context.Background(), worker, id)
			if err != nil {
				t.Errorf("claim by %s: %v", worker, err)
			}
			if ok {
				won <- worker
			}
		}()
	}
	wg.Wait()
	close(won)

	var winners []string
	for w := range won {
		winners = append(winners, w)
	}
	if len(winners) != 1 {
		t.Fatalf("winners = %v; want exactly 1: ClaimOutboxMessage must be fenced on status=PENDING", winners)
	}
	if r := readOutbox(t, db, id); r.status != "CLAIMED" || r.claimedBy.String != winners[0] {
		t.Errorf("row = %s/%s; want CLAIMED by the winner %s", r.status, r.claimedBy.String, winners[0])
	}
}

func TestClaimOutboxMessageNeedsAPendingRow(t *testing.T) {
	for _, st := range []domain.OutboxStatus{domain.OutboxClaimed, domain.OutboxPublished, domain.OutboxBlocked} {
		t.Run(string(st), func(t *testing.T) {
			s, db := newStore(t)
			id := seedOutbox(t, db, outboxSeed{status: st, claimedBy: "old", claimedAt: created})

			ok, err := s.ClaimOutboxMessage(context.Background(), "w1", id)
			if err != nil || ok {
				t.Errorf("claim = %v, %v; want false: only a PENDING row can be claimed", ok, err)
			}
			if r := readOutbox(t, db, id); r.status != string(st) || r.claimedBy.String != "old" {
				t.Errorf("row = %s/%s; want it unchanged at %s/old", r.status, r.claimedBy.String, st)
			}
		})
	}
}

// ---- MarkPublished, ReleaseClaim, MarkBlocked (fenced by claimed_by and CLAIMED)

func TestMarkPublishedByTheOwner(t *testing.T) {
	s, db := newStore(t)
	id := seedOutbox(t, db, outboxSeed{status: domain.OutboxClaimed, claimedBy: "w1", claimedAt: now, retries: 1})

	ok, err := s.MarkPublished(context.Background(), id, "w1")
	if err != nil || !ok {
		t.Fatalf("mark published = %v, %v; want true for the claim owner", ok, err)
	}
	if r := readOutbox(t, db, id); r.status != "PUBLISHED" || r.retries != 1 || r.claimedBy.Valid || r.claimedAt.Valid {
		t.Errorf("row = %s/retries=%d/claim=%v,%v; want PUBLISHED, retries kept, claim cleared", r.status, r.retries, r.claimedBy, r.claimedAt)
	}
}

// A worker whose claim was recovered and re-claimed by another worker must not mark the
// new owner's row: that would hide the second publish.
func TestMarkPublishedByAStaleWorkerChangesNothing(t *testing.T) {
	s, db := newStore(t)
	id := seedOutbox(t, db, outboxSeed{status: domain.OutboxClaimed, claimedBy: "w2", claimedAt: now, retries: 1})

	ok, err := s.MarkPublished(context.Background(), id, "w1")
	if err != nil || ok {
		t.Errorf("mark published = %v, %v; want false: MarkPublished must be fenced on claimed_by", ok, err)
	}
	if r := readOutbox(t, db, id); r.status != "CLAIMED" || r.claimedBy.String != "w2" {
		t.Errorf("row = %s/%s; want CLAIMED by w2 untouched", r.status, r.claimedBy.String)
	}
}

func TestMarkPublishedNeedsAClaimedRow(t *testing.T) {
	s, db := newStore(t)
	// Every move out of CLAIMED clears claimed_by, so the status fence is defence in depth.
	// Seed the state it guards against directly: a non-CLAIMED row still naming this worker.
	id := seedOutbox(t, db, outboxSeed{status: domain.OutboxBlocked, claimedBy: "w1", claimedAt: now})

	ok, err := s.MarkPublished(context.Background(), id, "w1")
	if err != nil || ok {
		t.Errorf("mark published = %v, %v; want false: MarkPublished must be fenced on status=CLAIMED", ok, err)
	}
	if r := readOutbox(t, db, id); r.status != "BLOCKED" {
		t.Errorf("status = %s; want BLOCKED untouched", r.status)
	}
}

func TestReleaseClaimCountsTheAttempt(t *testing.T) {
	const maxRetries = 3
	cases := []struct {
		retries     int
		wantStatus  string
		wantRetries int
	}{
		{0, "PENDING", 1},
		{1, "PENDING", 2},
		{2, "BLOCKED", 3},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("retries=%d", c.retries), func(t *testing.T) {
			s, db := newStore(t)
			id := seedOutbox(t, db, outboxSeed{status: domain.OutboxClaimed, claimedBy: "w1", claimedAt: now, retries: c.retries})

			ok, err := s.ReleaseClaim(context.Background(), id, "w1", maxRetries)
			if err != nil || !ok {
				t.Fatalf("release = %v, %v; want true for the claim owner", ok, err)
			}
			r := readOutbox(t, db, id)
			if r.status != c.wantStatus || r.retries != c.wantRetries || r.claimedBy.Valid {
				t.Errorf("after release: %s/retries=%d/claimed=%v; want %s/retries=%d/unclaimed: BLOCKED once retries reach max_retries",
					r.status, r.retries, r.claimedBy.Valid, c.wantStatus, c.wantRetries)
			}
		})
	}
}

func TestReleaseClaimByAStaleWorkerChangesNothing(t *testing.T) {
	s, db := newStore(t)
	id := seedOutbox(t, db, outboxSeed{status: domain.OutboxClaimed, claimedBy: "w2", claimedAt: now, retries: 1})

	ok, err := s.ReleaseClaim(context.Background(), id, "w1", 3)
	if err != nil || ok {
		t.Errorf("release = %v, %v; want false: ReleaseClaim must be fenced on claimed_by", ok, err)
	}
	if r := readOutbox(t, db, id); r.status != "CLAIMED" || r.retries != 1 || r.claimedBy.String != "w2" {
		t.Errorf("row = %s/retries=%d/%s; want CLAIMED/1/w2 untouched", r.status, r.retries, r.claimedBy.String)
	}
}

func TestMarkBlockedIsFencedAndDoesNotCountARetry(t *testing.T) {
	s, db := newStore(t)
	owned := seedOutbox(t, db, outboxSeed{status: domain.OutboxClaimed, claimedBy: "w1", claimedAt: now, retries: 1})
	stolen := seedOutbox(t, db, outboxSeed{status: domain.OutboxClaimed, claimedBy: "w2", claimedAt: now})

	if ok, err := s.MarkBlocked(context.Background(), owned, "w1"); err != nil || !ok {
		t.Fatalf("mark blocked = %v, %v; want true for the claim owner", ok, err)
	}
	if r := readOutbox(t, db, owned); r.status != "BLOCKED" || r.retries != 1 || r.claimedBy.Valid {
		t.Errorf("row = %s/retries=%d/claimed=%v; want BLOCKED, retries kept at 1, claim cleared", r.status, r.retries, r.claimedBy.Valid)
	}

	if ok, err := s.MarkBlocked(context.Background(), stolen, "w1"); err != nil || ok {
		t.Errorf("mark blocked by a non-owner = %v, %v; want false: MarkBlocked must be fenced on claimed_by", ok, err)
	}
	if r := readOutbox(t, db, stolen); r.status != "CLAIMED" || r.claimedBy.String != "w2" {
		t.Errorf("row = %s/%s; want CLAIMED by w2 untouched", r.status, r.claimedBy.String)
	}
}

// ---- RecoverStaleOutbox (the outbox sweeper)

func TestRecoverStaleOutboxDemotesOnlyStaleClaims(t *testing.T) {
	s, db := newStore(t)
	cutoff := now.Add(-30 * time.Minute)
	stale := seedOutbox(t, db, outboxSeed{status: domain.OutboxClaimed, claimedBy: "dead", claimedAt: cutoff.Add(-time.Second)})
	staleLast := seedOutbox(t, db, outboxSeed{status: domain.OutboxClaimed, claimedBy: "dead", claimedAt: cutoff.Add(-time.Hour), retries: 2})
	atCutoff := seedOutbox(t, db, outboxSeed{status: domain.OutboxClaimed, claimedBy: "live", claimedAt: cutoff})
	fresh := seedOutbox(t, db, outboxSeed{status: domain.OutboxClaimed, claimedBy: "live", claimedAt: now})
	pending := seedOutbox(t, db, outboxSeed{status: domain.OutboxPending})

	n, err := s.RecoverStaleOutbox(context.Background(), cutoff, 3)
	if err != nil || n != 2 {
		t.Fatalf("recovered = %d, %v; want 2 (only CLAIMED rows older than the cutoff)", n, err)
	}
	want := map[int64]struct {
		status  string
		retries int
		claimed bool
	}{
		stale:     {"PENDING", 1, false},
		staleLast: {"BLOCKED", 3, false},
		atCutoff:  {"CLAIMED", 0, true},
		fresh:     {"CLAIMED", 0, true},
		pending:   {"PENDING", 0, false},
	}
	for id, w := range want {
		r := readOutbox(t, db, id)
		if r.status != w.status || r.retries != w.retries || r.claimedBy.Valid != w.claimed {
			t.Errorf("row %d = %s/retries=%d/claimed=%v; want %s/retries=%d/claimed=%v: recovery counts a dead claim as a failed attempt and must leave live claims alone",
				id, r.status, r.retries, r.claimedBy.Valid, w.status, w.retries, w.claimed)
		}
	}
}

// Every move out of CLAIMED clears claimed_by, so these status fences are defence in depth.
// Seed the state they guard against directly: a PUBLISHED row still naming w1.
func TestReleaseClaimAndMarkBlockedNeedAClaimedRow(t *testing.T) {
	ops := map[string]func(s *sqlite.Store, id int64) (bool, error){
		"ReleaseClaim": func(s *sqlite.Store, id int64) (bool, error) {
			return s.ReleaseClaim(context.Background(), id, "w1", 3)
		},
		"MarkBlocked": func(s *sqlite.Store, id int64) (bool, error) { return s.MarkBlocked(context.Background(), id, "w1") },
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			s, db := newStore(t)
			id := seedOutbox(t, db, outboxSeed{status: domain.OutboxPublished, claimedBy: "w1", claimedAt: now})

			ok, err := op(s, id)
			if err != nil || ok {
				t.Errorf("%s = %v, %v; want false: it must be fenced on status=CLAIMED", name, ok, err)
			}
			if r := readOutbox(t, db, id); r.status != "PUBLISHED" || r.retries != 0 {
				t.Errorf("row = %s/retries=%d; want PUBLISHED/0 untouched (a published event must never be re-queued)", r.status, r.retries)
			}
		})
	}
}

func TestRecoverStaleOutboxIgnoresRowsThatAreNotClaimed(t *testing.T) {
	s, db := newStore(t)
	cutoff := now.Add(-30 * time.Minute)
	published := seedOutbox(t, db, outboxSeed{status: domain.OutboxPublished, claimedBy: "dead", claimedAt: cutoff.Add(-time.Hour)})

	n, err := s.RecoverStaleOutbox(context.Background(), cutoff, 3)
	if err != nil || n != 0 {
		t.Errorf("recovered = %d, %v; want 0: RecoverStaleOutbox must be fenced on status=CLAIMED", n, err)
	}
	if r := readOutbox(t, db, published); r.status != "PUBLISHED" || r.retries != 0 {
		t.Errorf("row = %s/retries=%d; want PUBLISHED/0: recovery must never re-queue a published event", r.status, r.retries)
	}
}
