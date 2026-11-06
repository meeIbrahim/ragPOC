package main

import (
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
