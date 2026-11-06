package hashing

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

var sweepNow = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)

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
	s.now = func() time.Time { return sweepNow }
	return s
}

func TestSweeperRecoversClaimsOlderThanStaleThreshold(t *testing.T) {
	store := newFakeStore()
	s := testSweeper(store, discard)

	runSweeper(t, s, store, 1)

	got := store.recovers()[0]
	if want := sweepNow.Add(-DefaultConfig().StaleThreshold); !got.cutoff.Equal(want) || got.maxRetries != DefaultConfig().MaxRetries {
		t.Errorf("recover(cutoff=%v, max=%d); want cutoff = now - StaleThreshold = %v and max = MaxRetries %d",
			got.cutoff, got.maxRetries, want, DefaultConfig().MaxRetries)
	}
}

func TestSweeperLogsAnErrorAndKeepsSweeping(t *testing.T) {
	store := newFakeStore()
	store.errs["RecoverStaleIntents"] = errors.New("db locked")
	var buf bytes.Buffer
	s := testSweeper(store, slog.New(slog.NewTextHandler(&buf, nil)))

	runSweeper(t, s, store, 2) // a second pass proves the error did not end the loop

	if !strings.Contains(buf.String(), "db locked") {
		t.Errorf("log = %q; want the recovery error logged", buf.String())
	}
}

// The job must give up its claim before the sweep can hand it to another worker; otherwise
// two workers hash the same intent.
func TestDefaultJobTimeoutIsBelowStaleThreshold(t *testing.T) {
	if cfg := DefaultConfig(); cfg.JobTimeout >= cfg.StaleThreshold {
		t.Errorf("JobTimeout %v >= StaleThreshold %v; a live job could be recovered under it", cfg.JobTimeout, cfg.StaleThreshold)
	}
}

func TestNewWorkerIDIsUniqueAndNamesTheKind(t *testing.T) {
	a, b := NewWorkerID(), NewWorkerID()
	if a == b {
		t.Errorf("two worker ids are both %q; claims are fenced by id, so they must differ", a)
	}
	if !strings.Contains(a, "/hash/") {
		t.Errorf("worker id %q; want <host>/hash/<uuid> (L19 naming)", a)
	}
}
