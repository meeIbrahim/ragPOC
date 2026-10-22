//go:build e2e

// Package e2e drives the whole system through its public edges: the center's HTTP API, the two
// Redis streams, and (read-only) the center's SQLite file. It runs inside the compose network
// (see ../../../scripts/e2e.sh); the stub consumer's mode is set from outside before each
// scenario, so a subtest only asserts what that mode must lead to.
package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"center-service/internal/config"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	_ "modernc.org/sqlite"
)

// cfg is the same file every service in the stack reads (deploy/config.e2e.toml, mounted at
// /config/config.toml), so the streams, groups and timings asserted on cannot drift from it.
var cfg = mustLoadConfig()

var (
	ingestStream = cfg.Redis.Stream
	statusStream = cfg.Redis.StatusStream
	ingestGroup  = cfg.Redis.ConsumerGroup
	statusGroup  = cfg.Redis.StatusConsumerGroup

	maxAttempts = cfg.Ingestion.MaxAttempts
)

func mustLoadConfig() *config.Config {
	c, err := config.Load(getenv("CONFIG", "/config/config.toml"))
	if err != nil {
		panic(fmt.Sprintf("e2e: %v (set CONFIG, default /config/config.toml; the e2e runs inside compose, see scripts/e2e.sh)", err))
	}
	return c
}

var httpClient = &http.Client{Timeout: 15 * time.Second}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func centerURL() string { return getenv("CENTER_URL", "http://center:8080") }

// ---- HTTP

// upload runs the whole client flow: create the intent, PUT the bytes to the presigned URL
// (which points at the MinIO service, reachable from inside the compose network), confirm.
func upload(t *testing.T, filename string, content []byte) string {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"filename": filename})
	resp, err := httpClient.Post(centerURL()+"/upload", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /upload: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /upload = %d: %s", resp.StatusCode, raw)
	}
	var created struct {
		ID        string `json:"id"`
		UploadURL string `json:"upload_url"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("decode create response: %v (%s)", err, raw)
	}

	req, _ := http.NewRequest(http.MethodPut, created.UploadURL, bytes.NewReader(content))
	putResp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("PUT to presigned URL %s: %v", created.UploadURL, err)
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT to presigned URL = %d", putResp.StatusCode)
	}

	confirm, err := httpClient.Post(centerURL()+"/upload/"+created.ID+"/confirm", "application/json", nil)
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	confirm.Body.Close()
	if confirm.StatusCode != http.StatusAccepted {
		t.Fatalf("confirm = %d, want 202", confirm.StatusCode)
	}
	return created.ID
}

// uniqueContent makes bytes no earlier scenario used, so every scenario gets its own document
// hash (the document identity is the SHA-256 of the content).
func uniqueContent(label string) []byte {
	salt := make([]byte, 8)
	_, _ = rand.Read(salt)
	return []byte(fmt.Sprintf("%s %d %s", label, time.Now().UnixNano(), hex.EncodeToString(salt)))
}

func pollStatus(id string) string {
	resp, err := httpClient.Get(centerURL() + "/upload/" + id)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	var body struct {
		Status string `json:"status"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&body) != nil {
		return ""
	}
	return body.Status
}

// eventually polls until check returns true or the timeout passes; it never sleeps blindly.
func eventually(timeout time.Duration, check func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if check() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// waitFinal waits until GET /upload/{id} reports INDEXED or FAILED.
func waitFinal(t *testing.T, id string, timeout time.Duration) string {
	t.Helper()
	last := ""
	if !eventually(timeout, func() bool {
		last = pollStatus(id)
		return last == "INDEXED" || last == "FAILED"
	}) {
		t.Fatalf("upload %s did not reach INDEXED or FAILED within %v (last status %q)", id, timeout, last)
	}
	return last
}

// ---- SQLite (read-only)

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	path := getenv("CENTER_DB", "/data/go/center.db")
	// mode=ro: the test only looks. The volume itself is mounted read-write because a read-only
	// mount cannot create the WAL shared-memory file a reader needs.
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func hashOf(t *testing.T, db *sql.DB, uploadID string) string {
	t.Helper()
	var hash string
	if !eventually(30*time.Second, func() bool {
		var h sql.NullString
		if err := db.QueryRow(`SELECT hash FROM upload_intent WHERE uuid = ?`, uploadID).Scan(&h); err != nil {
			return false
		}
		hash = h.String
		return h.Valid && h.String != ""
	}) {
		t.Fatalf("upload %s was never hashed", uploadID)
	}
	return hash
}

type docRow struct {
	Status  string
	Retries int
}

func readDoc(db *sql.DB, hash string) (docRow, error) {
	var d docRow
	err := db.QueryRow(`SELECT status, retries FROM document_store WHERE hash = ?`, hash).Scan(&d.Status, &d.Retries)
	return d, err
}

func mustDoc(t *testing.T, db *sql.DB, hash string) docRow {
	t.Helper()
	d, err := readDoc(db, hash)
	if err != nil {
		t.Fatalf("document %s: %v", hash, err)
	}
	return d
}

// updatedAt is the row's last-write stamp. It moves on every UPDATE, so comparing it before and
// after a redelivery proves the center did not touch the row, even when the visible columns
// would have ended up the same.
func updatedAt(t *testing.T, db *sql.DB, hash string) string {
	t.Helper()
	var ts string
	if err := db.QueryRow(`SELECT updated_at FROM document_store WHERE hash = ?`, hash).Scan(&ts); err != nil {
		t.Fatalf("document %s updated_at: %v", hash, err)
	}
	return ts
}

func outboxCount(t *testing.T, db *sql.DB, hash string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox WHERE hash = ?`, hash).Scan(&n); err != nil {
		t.Fatalf("outbox count: %v", err)
	}
	return n
}

// ---- Redis

func newRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: getenv("REDIS_ADDR", "redis:6379")})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis: %v", err)
	}
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

// statusEvent is the part of an IngestionStatus payload the scenarios assert on.
type statusEvent struct {
	ID        string `json:"id"`
	Hash      string `json:"hash"`
	Status    string `json:"status"`
	Attempts  int    `json:"attempts"`
	Retryable *bool  `json:"retryable"`
	Reason    string `json:"reason"`
	Payload   string `json:"-"`
}

func statusEventsFor(t *testing.T, rdb *redis.Client, hash string) []statusEvent {
	t.Helper()
	entries, err := rdb.XRange(context.Background(), statusStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE %s: %v", statusStream, err)
	}
	var events []statusEvent
	for _, e := range entries {
		raw, _ := e.Values["payload"].(string)
		var ev statusEvent
		if json.Unmarshal([]byte(raw), &ev) == nil && ev.Hash == hash {
			ev.Payload = raw
			events = append(events, ev)
		}
	}
	return events
}

// publishedAttempts is the `attempts` of every ingest event the outbox worker published for hash.
func publishedAttempts(t *testing.T, rdb *redis.Client, hash string) []int {
	t.Helper()
	entries, err := rdb.XRange(context.Background(), ingestStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE %s: %v", ingestStream, err)
	}
	var attempts []int
	for _, e := range entries {
		raw, _ := e.Values["payload"].(string)
		var ev struct {
			Hash     string `json:"hash"`
			Attempts int    `json:"attempts"`
		}
		if json.Unmarshal([]byte(raw), &ev) == nil && ev.Hash == hash {
			attempts = append(attempts, ev.Attempts)
		}
	}
	sort.Ints(attempts)
	return attempts
}

func addStatusEntry(t *testing.T, rdb *redis.Client, payload string) {
	t.Helper()
	err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: statusStream,
		Values: map[string]any{"event_type": "IngestionStatus", "payload": payload},
	}).Err()
	if err != nil {
		t.Fatalf("XADD %s: %v", statusStream, err)
	}
}

// failedPayload builds a FAILED status event as the Python service would publish it.
func failedPayload(hash string, attempts int, retryable bool, reason string) string {
	raw, _ := json.Marshal(map[string]any{
		"id": uuid.NewString(), "hash": hash, "status": "FAILED", "attempts": attempts,
		"retryable": retryable, "reason": reason,
		"created_at": time.Now().UTC().Format(time.RFC3339Nano), "schema_version": 1,
	})
	return string(raw)
}

// settleStatusGroup waits until the center has read and acknowledged everything on the status
// stream, then a moment longer so a late write would show up.
func settleStatusGroup(t *testing.T, rdb *redis.Client) {
	t.Helper()
	ctx := context.Background()
	if !eventually(20*time.Second, func() bool {
		groups, err := rdb.XInfoGroups(ctx, statusStream).Result()
		if err != nil {
			return false
		}
		for _, g := range groups {
			if g.Name == statusGroup {
				return g.Pending == 0 && g.Lag == 0
			}
		}
		return false
	}) {
		t.Fatalf("the center never caught up on %s", statusStream)
	}
	time.Sleep(time.Second)
}

func resetStreams(t *testing.T, rdb *redis.Client) {
	t.Helper()
	// Both services create their group again when it disappears (NOGROUP), so deleting the
	// streams between scenarios also exercises that path.
	if err := rdb.Del(context.Background(), ingestStream, statusStream).Err(); err != nil {
		t.Fatalf("reset streams: %v", err)
	}
}

// consumerNames watches the ingest group for the whole wait and returns every consumer name it
// saw: each process start registers a new one.
type consumerWatcher struct {
	names map[string]bool
	stop  chan struct{}
	done  chan struct{}
}

func watchConsumers(rdb *redis.Client) *consumerWatcher {
	w := &consumerWatcher{names: map[string]bool{}, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for {
			consumers, err := rdb.XInfoConsumers(context.Background(), ingestStream, ingestGroup).Result()
			if err == nil {
				for _, c := range consumers {
					w.names[c.Name] = true
				}
			}
			select {
			case <-w.stop:
				return
			case <-time.After(300 * time.Millisecond):
			}
		}
	}()
	return w
}

func (w *consumerWatcher) finish() []string {
	close(w.stop)
	<-w.done
	var names []string
	for n := range w.names {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// ---- failure report

// scenario collects the document hashes a subtest touched, so a failure can print everything
// needed to debug it without rerunning.
type scenario struct {
	t      *testing.T
	db     *sql.DB
	rdb    *redis.Client
	hashes []string
}

func newScenario(t *testing.T) *scenario {
	t.Helper()
	s := &scenario{t: t, db: openDB(t), rdb: newRedis(t)}
	t.Cleanup(s.dumpOnFailure)
	return s
}

func (s *scenario) track(hash string) string {
	s.hashes = append(s.hashes, hash)
	return hash
}

func (s *scenario) dumpOnFailure() {
	if !s.t.Failed() {
		return
	}
	ctx := context.Background()
	var b strings.Builder
	for _, hash := range s.hashes {
		doc, err := readDoc(s.db, hash)
		fmt.Fprintf(&b, "document %s: %+v (err %v)\n", hash[:12], doc, err)
		rows, err := s.db.Query(`SELECT id, status, retries FROM outbox WHERE hash = ? ORDER BY id`, hash)
		if err == nil {
			for rows.Next() {
				var id int64
				var status string
				var retries int
				_ = rows.Scan(&id, &status, &retries)
				fmt.Fprintf(&b, "  outbox %d: %s retries=%d\n", id, status, retries)
			}
			rows.Close()
		}
	}
	for _, stream := range []string{ingestStream, statusStream} {
		entries, _ := s.rdb.XRange(ctx, stream, "-", "+").Result()
		fmt.Fprintf(&b, "stream %s (%d entries):\n", stream, len(entries))
		for _, e := range entries {
			fmt.Fprintf(&b, "  %s %v\n", e.ID, e.Values["payload"])
		}
	}
	for stream, group := range map[string]string{ingestStream: ingestGroup, statusStream: statusGroup} {
		consumers, err := s.rdb.XInfoConsumers(ctx, stream, group).Result()
		fmt.Fprintf(&b, "consumers of %s/%s (err %v):\n", stream, group, err)
		for _, c := range consumers {
			fmt.Fprintf(&b, "  %s pending=%d idle=%v\n", c.Name, c.Pending, c.Idle)
		}
	}
	s.t.Logf("---- state at failure ----\n%s", b.String())
}
