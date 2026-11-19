package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"center-service/internal/config"
	"center-service/internal/hashing"
	"center-service/internal/outbox"
)

func TestParseFault(t *testing.T) {
	cases := map[string]fault{
		"":                          {},
		"hash-hang":                 {hashHang: true},
		"outbox-hang-after-publish": {outboxHangAfterPublish: true},
	}
	for v, want := range cases {
		got, err := parseFault(v)
		if err != nil || got != want {
			t.Errorf("parseFault(%q) = %+v, %v; want %+v", v, got, err, want)
		}
	}
}

// A typo must stop the center instead of silently running the e2e without its fault.
func TestParseFaultRejectsAnUnknownValueByName(t *testing.T) {
	for _, v := range []string{"hash_hang", "HASH-HANG", " hash-hang", "outbox-hang"} {
		_, err := parseFault(v)
		if err == nil || !strings.Contains(err.Error(), `"`+v+`"`) {
			t.Errorf("parseFault(%q) err = %v; want an error naming the value", v, err)
		}
	}
}

// The workers used to run on hashing/outbox.DefaultConfig(); the shipped config.toml must keep
// those values, and the defaults (still asserted on by the sweeper tests) must not drift from it.
func TestRepoConfigKeepsTheWorkerDefaults(t *testing.T) {
	cfg, err := config.Load(filepath.Join("..", "..", "..", "..", "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if got := hashingConfig(cfg.Go.Hashing, fault{}); got != hashing.DefaultConfig() {
		t.Errorf("[go.hashing] = %+v; want hashing.DefaultConfig() %+v", got, hashing.DefaultConfig())
	}
	if got := outboxConfig(cfg.Go.Outbox, fault{}); got != outbox.DefaultConfig() {
		t.Errorf("[go.outbox] = %+v; want outbox.DefaultConfig() %+v", got, outbox.DefaultConfig())
	}
}

func TestWorkerConfigsComeFromTheTOMLTables(t *testing.T) {
	w := config.WorkerConfig{
		Interval: config.Duration(time.Second), BatchSize: 7, JobTimeout: config.Duration(3 * time.Second),
		MaxRetries: 12, RecoveryInterval: config.Duration(2 * time.Second), StaleThreshold: config.Duration(10 * time.Second),
	}

	h := hashingConfig(w, fault{hashHang: true})
	if h != (hashing.Config{Interval: time.Second, BatchSize: 7, JobTimeout: 3 * time.Second, MaxRetries: 12,
		RecoveryInterval: 2 * time.Second, StaleThreshold: 10 * time.Second, HangAfterClaim: true}) {
		t.Errorf("hashing config = %+v; want every [go.hashing] key and the hash-hang fault", h)
	}

	o := outboxConfig(config.OutboxConfig{WorkerConfig: w, SchemaVersion: 1}, fault{})
	if o != (outbox.Config{Interval: time.Second, BatchSize: 7, JobTimeout: 3 * time.Second, MaxRetries: 12,
		RecoveryInterval: 2 * time.Second, StaleThreshold: 10 * time.Second, SchemaVersion: 1}) {
		t.Errorf("outbox config = %+v; want every [go.outbox] key and no fault", o)
	}
	if outboxConfig(config.OutboxConfig{WorkerConfig: w, SchemaVersion: 1}, fault{outboxHangAfterPublish: true}).HangAfterPublish != true {
		t.Error("outbox-hang-after-publish did not reach the outbox config")
	}
}

// compose-demo D5: run() loads the config through loadConfig before it opens the database or
// starts a worker, so a bad static_dir stops the API without claiming rows.
func TestLoadConfigChecksStaticDir(t *testing.T) {
	repo, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	withDir := func(t *testing.T, dir string) string {
		t.Helper()
		body := strings.Replace(string(repo), `static_dir=""`, `static_dir="`+dir+`"`, 1)
		if body == string(repo) {
			t.Fatal(`config.toml has no static_dir="" line to replace`)
		}
		path := filepath.Join(t.TempDir(), "config.toml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	bad := filepath.Join(t.TempDir(), "no-such-dir")
	if _, err := loadConfig(withDir(t, bad)); err == nil || !strings.Contains(err.Error(), "go.ui.static_dir") {
		t.Fatalf("loadConfig with a missing static_dir = %v; want a go.ui.static_dir error", err)
	}

	site := t.TempDir()
	if err := os.WriteFile(filepath.Join(site, "index.html"), []byte("<html></html>"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(withDir(t, site))
	if err != nil {
		t.Fatalf("loadConfig with a valid static_dir = %v", err)
	}
	if cfg.Go.UI.StaticDir != site {
		t.Errorf("static_dir = %q; want %q", cfg.Go.UI.StaticDir, site)
	}
}
