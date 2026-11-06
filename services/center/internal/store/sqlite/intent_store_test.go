package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"center-service/internal/domain"
)

// intentRow is what the tests read back from upload_intent.
type intentRow struct {
	status     string
	retries    int
	claimedBy  sql.NullString
	claimedAt  sql.NullTime
	uploadedAt sql.NullTime
	hash       sql.NullString
}

type intentSeed struct {
	status    domain.IntentState
	createdAt time.Time
	expiresAt time.Time
	claimedBy string // empty: no claim
	claimedAt time.Time
	retries   int
}

// seedIntent inserts an intent directly, so a test can start from any state.
func seedIntent(t *testing.T, db *sql.DB, id string, s intentSeed) {
	t.Helper()
	if s.createdAt.IsZero() {
		s.createdAt = created
	}
	if s.expiresAt.IsZero() {
		s.expiresAt = now.Add(time.Hour)
	}
	var claimedBy, claimedAt any
	if s.claimedBy != "" {
		claimedBy, claimedAt = s.claimedBy, s.claimedAt
	}
	_, err := db.Exec(`INSERT INTO upload_intent
		(uuid, file_name, temp_bucket_path, status, claimed_by, claimed_at, retries, created_at, expires_at)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		id, id+".txt", id, s.status.String(), claimedBy, claimedAt, s.retries, s.createdAt, s.expiresAt)
	if err != nil {
		t.Fatalf("seed intent %s: %v", id, err)
	}
}

func readIntent(t *testing.T, db *sql.DB, id string) intentRow {
	t.Helper()
	var r intentRow
	err := db.QueryRow(`SELECT status, retries, claimed_by, claimed_at, uploaded_at, hash FROM upload_intent WHERE uuid = ?`, id).
		Scan(&r.status, &r.retries, &r.claimedBy, &r.claimedAt, &r.uploadedAt, &r.hash)
	if err != nil {
		t.Fatalf("read intent %s: %v", id, err)
	}
	return r
}

func ptr[T any](v T) *T { return &v }

// ---- UpdateIntentToUploaded (the confirm gate)

func TestUpdateIntentToUploadedWinsOnceFromWaiting(t *testing.T) {
	s, db := newStore(t)
	seedIntent(t, db, "i1", intentSeed{status: domain.IntentWaiting})
	ctx := context.Background()

	n, err := s.UpdateIntentToUploaded(ctx, "i1", ptr(now))
	if err != nil || n != 1 {
		t.Fatalf("first confirm = %d, %v; want 1 row: a WAITING intent inside its window must pass the gate", n, err)
	}
	r := readIntent(t, db, "i1")
	if r.status != "UPLOADED" || !r.uploadedAt.Valid || !r.uploadedAt.Time.Equal(now) {
		t.Errorf("after confirm: status %s, uploaded_at %v; want UPLOADED at %v", r.status, r.uploadedAt, now)
	}

	n, err = s.UpdateIntentToUploaded(ctx, "i1", ptr(now.Add(time.Second)))
	if err != nil || n != 0 {
		t.Errorf("second confirm = %d, %v; want 0 rows: the gate is WAITING-only, so a repeat must not re-stamp the intent", n, err)
	}
	if r := readIntent(t, db, "i1"); !r.uploadedAt.Time.Equal(now) {
		t.Errorf("uploaded_at = %v after a repeat confirm; want it kept at %v", r.uploadedAt.Time, now)
	}
}

func TestUpdateIntentToUploadedRefusesAnExpiredIntent(t *testing.T) {
	for name, expiresAt := range map[string]time.Time{
		"past expiry":    now.Add(-time.Second),
		"exactly expiry": now, // the gate needs expires_at > now
	} {
		t.Run(name, func(t *testing.T) {
			s, db := newStore(t)
			seedIntent(t, db, "i1", intentSeed{status: domain.IntentWaiting, expiresAt: expiresAt})

			n, err := s.UpdateIntentToUploaded(context.Background(), "i1", ptr(now))
			if err != nil || n != 0 {
				t.Errorf("confirm = %d, %v; want 0 rows: an intent at or past expires_at must not pass the gate", n, err)
			}
			if r := readIntent(t, db, "i1"); r.status != "WAITING" {
				t.Errorf("status = %s; want WAITING (the gate refuses without writing)", r.status)
			}
		})
	}
}

// ---- UpdateIntentToExpired

func TestUpdateIntentToExpiredOnlyTouchesWaitingPastExpiry(t *testing.T) {
	cases := []struct {
		name      string
		status    domain.IntentState
		expiresAt time.Time
		wantRows  int64
		want      string
	}{
		{"waiting past expiry", domain.IntentWaiting, now.Add(-time.Minute), 1, "EXPIRED"},
		{"waiting exactly at expiry", domain.IntentWaiting, now, 1, "EXPIRED"},
		{"waiting inside its window", domain.IntentWaiting, now.Add(time.Minute), 0, "WAITING"},
		{"uploaded past expiry", domain.IntentUploaded, now.Add(-time.Minute), 0, "UPLOADED"},
		{"hashed past expiry", domain.IntentHashed, now.Add(-time.Minute), 0, "HASHED"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, db := newStore(t)
			seedIntent(t, db, "i1", intentSeed{status: c.status, expiresAt: c.expiresAt})

			n, err := s.UpdateIntentToExpired(context.Background(), "i1", ptr(now))
			if err != nil || n != c.wantRows {
				t.Errorf("expire = %d, %v; want %d: only a WAITING intent at or past expires_at may expire", n, err, c.wantRows)
			}
			if r := readIntent(t, db, "i1"); r.status != c.want {
				t.Errorf("status = %s, want %s", r.status, c.want)
			}
		})
	}
}

// ---- GetIntentsForHashing

func TestGetIntentsForHashingReturnsOnlyUploadedOldestFirst(t *testing.T) {
	s, db := newStore(t)
	seedIntent(t, db, "new", intentSeed{status: domain.IntentUploaded, createdAt: created.Add(3 * time.Minute)})
	seedIntent(t, db, "old", intentSeed{status: domain.IntentUploaded, createdAt: created.Add(1 * time.Minute)})
	seedIntent(t, db, "mid", intentSeed{status: domain.IntentUploaded, createdAt: created.Add(2 * time.Minute)})
	for i, st := range []domain.IntentState{domain.IntentWaiting, domain.IntentHashing, domain.IntentHashed, domain.IntentExpired, domain.IntentFailed} {
		seedIntent(t, db, fmt.Sprintf("other%d", i), intentSeed{status: st, createdAt: created})
	}

	got, err := s.GetIntentsForHashing(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, i := range got {
		ids = append(ids, i.ID)
	}
	if fmt.Sprint(ids) != "[old mid]" {
		t.Errorf("intents = %v; want [old mid]: only UPLOADED, oldest created_at first, at most limit", ids)
	}
	for _, i := range got {
		if i.Status != domain.IntentUploaded {
			t.Errorf("intent %s has status %v; the hash worker must only see UPLOADED intents", i.ID, i.Status)
		}
	}
}

// ---- UpdateIntentToHashing (the hash worker's claim)

func TestUpdateIntentToHashingHasExactlyOneWinner(t *testing.T) {
	s, db := newStore(t)
	seedIntent(t, db, "i1", intentSeed{status: domain.IntentUploaded})

	const claimers = 8
	var wg sync.WaitGroup
	won := make(chan string, claimers)
	for i := range claimers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker := fmt.Sprintf("w%d", i)
			n, err := s.UpdateIntentToHashing(context.Background(), "i1", &worker, ptr(now))
			if err != nil {
				t.Errorf("claim by %s: %v", worker, err)
			}
			if n == 1 {
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
		t.Fatalf("winners = %v; want exactly 1: the claim must be fenced on status=UPLOADED", winners)
	}
	r := readIntent(t, db, "i1")
	if r.status != "HASHING" || r.claimedBy.String != winners[0] || !r.claimedAt.Time.Equal(now) {
		t.Errorf("row = %s/%s/%v; want HASHING owned by the winner %s at %v", r.status, r.claimedBy.String, r.claimedAt.Time, winners[0], now)
	}
}

func TestUpdateIntentToHashingNeedsAnUploadedIntent(t *testing.T) {
	for _, st := range []domain.IntentState{domain.IntentWaiting, domain.IntentHashing, domain.IntentHashed, domain.IntentFailed} {
		t.Run(st.String(), func(t *testing.T) {
			s, db := newStore(t)
			seedIntent(t, db, "i1", intentSeed{status: st, claimedBy: "old", claimedAt: created})

			n, err := s.UpdateIntentToHashing(context.Background(), "i1", ptr("w1"), ptr(now))
			if err != nil || n != 0 {
				t.Errorf("claim = %d, %v; want 0 rows: only an UPLOADED intent can be claimed", n, err)
			}
			if r := readIntent(t, db, "i1"); r.status != st.String() {
				t.Errorf("status = %s, want it unchanged at %s", r.status, st)
			}
		})
	}
}

// ---- MarkIntentFailed and ReleaseIntentClaim (fenced by claimed_by)

func TestMarkIntentFailedIsFencedByTheClaim(t *testing.T) {
	cases := []struct {
		name     string
		status   domain.IntentState
		worker   string
		wantRows int64
		want     string
	}{
		{"owner", domain.IntentHashing, "w1", 1, "FAILED"},
		{"another worker", domain.IntentHashing, "w2", 0, "HASHING"},
		{"owner after the claim moved on", domain.IntentUploaded, "w1", 0, "UPLOADED"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, db := newStore(t)
			seedIntent(t, db, "i1", intentSeed{status: c.status, claimedBy: "w1", claimedAt: now})

			n, err := s.MarkIntentFailed(context.Background(), "i1", c.worker)
			if err != nil || n != c.wantRows {
				t.Errorf("mark failed = %d, %v; want %d: only the worker holding the HASHING claim may fail the intent", n, err, c.wantRows)
			}
			r := readIntent(t, db, "i1")
			if r.status != c.want {
				t.Errorf("status = %s, want %s", r.status, c.want)
			}
			if c.wantRows == 1 && (r.claimedBy.Valid || r.claimedAt.Valid) {
				t.Errorf("claim = %v/%v after failing; want it cleared", r.claimedBy, r.claimedAt)
			}
			if c.wantRows == 1 && r.retries != 0 {
				t.Errorf("retries = %d; want 0: a permanent failure must not consume a retry", r.retries)
			}
		})
	}
}

func TestReleaseIntentClaimCountsTheAttempt(t *testing.T) {
	const maxRetries = 3
	cases := []struct {
		name        string
		retries     int
		wantStatus  string
		wantRetries int
	}{
		{"first failure", 0, "UPLOADED", 1},
		{"second failure", 1, "UPLOADED", 2},
		{"last allowed failure", 2, "FAILED", 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, db := newStore(t)
			seedIntent(t, db, "i1", intentSeed{status: domain.IntentHashing, claimedBy: "w1", claimedAt: now, retries: c.retries})

			n, err := s.ReleaseIntentClaim(context.Background(), "i1", "w1", maxRetries)
			if err != nil || n != 1 {
				t.Fatalf("release = %d, %v; want 1 row for the claim owner", n, err)
			}
			r := readIntent(t, db, "i1")
			if r.status != c.wantStatus || r.retries != c.wantRetries {
				t.Errorf("after release: %s/retries=%d; want %s/retries=%d: FAILED once retries reach max_retries, else back to UPLOADED",
					r.status, r.retries, c.wantStatus, c.wantRetries)
			}
			if r.claimedBy.Valid || r.claimedAt.Valid {
				t.Errorf("claim = %v/%v after release; want it cleared so another worker can claim", r.claimedBy, r.claimedAt)
			}
		})
	}
}

func TestReleaseIntentClaimByAnotherWorkerChangesNothing(t *testing.T) {
	s, db := newStore(t)
	seedIntent(t, db, "i1", intentSeed{status: domain.IntentHashing, claimedBy: "w2", claimedAt: now, retries: 1})

	n, err := s.ReleaseIntentClaim(context.Background(), "i1", "w1", 3)
	if err != nil || n != 0 {
		t.Errorf("release = %d, %v; want 0 rows: a worker that lost its claim must not release the new owner's", n, err)
	}
	if r := readIntent(t, db, "i1"); r.status != "HASHING" || r.retries != 1 || r.claimedBy.String != "w2" {
		t.Errorf("row = %s/retries=%d/%s; want HASHING/retries=1/w2 untouched", r.status, r.retries, r.claimedBy.String)
	}
}

// ---- RecoverStaleIntents (the hash sweeper)

func TestRecoverStaleIntentsDemotesOnlyStaleClaims(t *testing.T) {
	s, db := newStore(t)
	cutoff := now.Add(-5 * time.Minute)
	seedIntent(t, db, "stale", intentSeed{status: domain.IntentHashing, claimedBy: "dead", claimedAt: cutoff.Add(-time.Second)})
	seedIntent(t, db, "stale-last", intentSeed{status: domain.IntentHashing, claimedBy: "dead", claimedAt: cutoff.Add(-time.Hour), retries: 2})
	seedIntent(t, db, "at-cutoff", intentSeed{status: domain.IntentHashing, claimedBy: "live", claimedAt: cutoff})
	seedIntent(t, db, "fresh", intentSeed{status: domain.IntentHashing, claimedBy: "live", claimedAt: now})
	seedIntent(t, db, "uploaded", intentSeed{status: domain.IntentUploaded, claimedBy: "old", claimedAt: cutoff.Add(-time.Hour)})

	n, err := s.RecoverStaleIntents(context.Background(), cutoff, 3)
	if err != nil || n != 2 {
		t.Fatalf("recovered = %d, %v; want 2 (only HASHING claims older than the cutoff)", n, err)
	}
	want := map[string]struct {
		status  string
		retries int
		claimed bool
	}{
		"stale":      {"UPLOADED", 1, false},
		"stale-last": {"FAILED", 3, false},
		"at-cutoff":  {"HASHING", 0, true},
		"fresh":      {"HASHING", 0, true},
		"uploaded":   {"UPLOADED", 0, true},
	}
	for id, w := range want {
		r := readIntent(t, db, id)
		if r.status != w.status || r.retries != w.retries || r.claimedBy.Valid != w.claimed {
			t.Errorf("%s = %s/retries=%d/claimed=%v; want %s/retries=%d/claimed=%v: recovery counts a dead claim as a failed attempt and must leave live claims alone",
				id, r.status, r.retries, r.claimedBy.Valid, w.status, w.retries, w.claimed)
		}
	}
}

// Every move out of HASHING clears claimed_by, so the status fences below are defence in
// depth. Seed the state they guard against directly: a non-HASHING intent still naming w1.
func TestReleaseIntentClaimNeedsAHashingIntent(t *testing.T) {
	s, db := newStore(t)
	seedIntent(t, db, "i1", intentSeed{status: domain.IntentUploaded, claimedBy: "w1", claimedAt: now, retries: 1})

	n, err := s.ReleaseIntentClaim(context.Background(), "i1", "w1", 3)
	if err != nil || n != 0 {
		t.Errorf("release = %d, %v; want 0 rows: ReleaseIntentClaim must be fenced on status=HASHING", n, err)
	}
	if r := readIntent(t, db, "i1"); r.status != "UPLOADED" || r.retries != 1 {
		t.Errorf("row = %s/retries=%d; want UPLOADED/1 untouched", r.status, r.retries)
	}
}
