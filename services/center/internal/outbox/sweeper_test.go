package outbox

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// runSweeper runs s until the store has seen `calls` recovery passes, then stops it.
func runSweeper(t *testing.T, s *Sweeper, store *fakeStore, calls int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	deadline := time.Now().Add(2 * time.Second)
	for len(store.recovers()) < calls {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("sweeper made %d recovery passes in 2s; want %d", len(store.recovers()), calls)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Sweeper.Run did not return after cancel")
	}
}

func testSweeper(store *fakeStore, logger *slog.Logger) *Sweeper {
	cfg := DefaultConfig()
	cfg.RecoveryInterval = time.Millisecond
	s := NewSweeper(cfg, store, logger)
	s.now = func() time.Time { return fixedNow }
	return s
}

func TestSweeperRecoversClaimsOlderThanStaleThreshold(t *testing.T) {
	store := newFakeStore()

	runSweeper(t, testSweeper(store, discard), store, 1)

	got := store.recovers()[0]
	if want := fixedNow.Add(-DefaultConfig().StaleThreshold); !got.cutoff.Equal(want) || got.maxRetries != DefaultConfig().MaxRetries {
		t.Errorf("recover(cutoff=%v, max=%d); want cutoff = now - StaleThreshold = %v and max = MaxRetries %d",
			got.cutoff, got.maxRetries, want, DefaultConfig().MaxRetries)
	}
}

func TestSweeperLogsAnErrorAndKeepsSweeping(t *testing.T) {
	store := newFakeStore()
	store.errs["RecoverStaleOutbox"] = errors.New("db locked")
	var buf bytes.Buffer

	runSweeper(t, testSweeper(store, slog.New(slog.NewTextHandler(&buf, nil))), store, 2) // a second pass proves the loop survived

	if !strings.Contains(buf.String(), "db locked") {
		t.Errorf("log = %q; want the recovery error logged", buf.String())
	}
}

// A publish must give up its claim before the sweep can hand the row to another worker;
// otherwise two workers publish the same event.
func TestDefaultJobTimeoutIsBelowStaleThreshold(t *testing.T) {
	if cfg := DefaultConfig(); cfg.JobTimeout >= cfg.StaleThreshold {
		t.Errorf("JobTimeout %v >= StaleThreshold %v; a live publish could be recovered under it", cfg.JobTimeout, cfg.StaleThreshold)
	}
}

func TestNewWorkerIdIsUniqueAndNamesTheKind(t *testing.T) {
	a, b := NewWorkerId(), NewWorkerId()
	if a == b {
		t.Errorf("two worker ids are both %q; claims are fenced by id, so they must differ", a)
	}
	if !strings.Contains(a, "/outbox/") {
		t.Errorf("worker id %q; want <host>/outbox/<uuid> (L19 naming)", a)
	}
}
