package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const validTOML = `
[minio]
url="localhost:9000"
access_key="minio_user"
secret_key="minio_password"
upload_bucket="upload-bucket"
ingestion_bucket="ingestion-bucket"

[sqlite]
dir="./sqlite_db"

[qdrant]
url="http://localhost:6333"

[rag_grpc]
port=50051

[rag]
collection="pdf-fixed"
embedding_model="qwen3-embedding"
embedding_dim=4096
llm_model="openai/gpt-oss-120b"
top_k=5
chunk_size=0
chunk_overlap=0
call_timeout="60s"

[rag.consumer]
job_timeout="15m"
watchdog_grace="2m"
min_idle="20m"
recovery_interval="1m"
poison_threshold=3
consumer_cleanup_idle="1h"

[redis]
host="localhost"
port=6379
stream="ingest.docs"
consumer_group="rag-pdf-fixed"
stream_max_len=324000
status_stream="ingest.status"
status_consumer_group="center-status"

[ingestion]
max_attempts=3
allowed_extensions=[".pdf", ".txt", ".md"]

[go]
port=8080
sqlite_dir="./go_sqlite_db"

[go.status_consumer]
entry_timeout="5s"
min_idle="60s"
recovery_interval="30s"
poison_threshold=3
consumer_cleanup_idle="1h"
`

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadParsesAllSections(t *testing.T) {
	cfg, err := Load(writeConfig(t, validTOML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RAGGRPC.Port != 50051 {
		t.Errorf("RAGGRPC.Port = %d", cfg.RAGGRPC.Port)
	}
	if cfg.RAG.EmbeddingModel != "qwen3-embedding" || cfg.RAG.EmbeddingDim != 4096 {
		t.Errorf("RAG embedding = %q/%d", cfg.RAG.EmbeddingModel, cfg.RAG.EmbeddingDim)
	}
	if cfg.RAG.CallTimeout.Std() != 60*time.Second {
		t.Errorf("RAG.CallTimeout = %v", cfg.RAG.CallTimeout.Std())
	}
	c := cfg.RAG.Consumer
	if c.JobTimeout.Std() != 15*time.Minute || c.WatchdogGrace.Std() != 2*time.Minute || c.MinIdle.Std() != 20*time.Minute {
		t.Errorf("RAG.Consumer timings = %v/%v/%v", c.JobTimeout.Std(), c.WatchdogGrace.Std(), c.MinIdle.Std())
	}
	if c.PoisonThreshold != 3 || c.ConsumerCleanupIdle.Std() != time.Hour {
		t.Errorf("RAG.Consumer poison/cleanup = %d/%v", c.PoisonThreshold, c.ConsumerCleanupIdle.Std())
	}
	if cfg.Redis.StatusStream != "ingest.status" || cfg.Redis.StatusConsumerGroup != "center-status" {
		t.Errorf("Redis status = %q/%q", cfg.Redis.StatusStream, cfg.Redis.StatusConsumerGroup)
	}
	if cfg.Redis.StreamMaxLen != 324000 {
		t.Errorf("Redis.StreamMaxLen = %d", cfg.Redis.StreamMaxLen)
	}
	if cfg.Ingestion.MaxAttempts != 3 || len(cfg.Ingestion.AllowedExtensions) != 3 {
		t.Errorf("Ingestion = %+v", cfg.Ingestion)
	}
	s := cfg.Go.StatusConsumer
	if s.EntryTimeout.Std() != 5*time.Second || s.MinIdle.Std() != 60*time.Second ||
		s.RecoveryInterval.Std() != 30*time.Second || s.PoisonThreshold != 3 || s.ConsumerCleanupIdle.Std() != time.Hour {
		t.Errorf("Go.StatusConsumer = %+v", s)
	}
}

func TestLoadRealConfig(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "..", "..", "config.toml"))
	if err != nil {
		t.Fatalf("repo config.toml: %v", err)
	}
	if len(cfg.UnknownKeys) != 0 {
		t.Fatalf("repo config.toml has keys Go does not model: %v", cfg.UnknownKeys)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.toml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadRejectsInvalid(t *testing.T) {
	tests := []struct {
		name    string
		old     string
		new     string
		wantErr string
	}{
		{"job_timeout + grace == min_idle", `min_idle="20m"`, `min_idle="17m"`, "rag.consumer.min_idle"},
		{"job_timeout + grace > min_idle", `min_idle="20m"`, `min_idle="10m"`, "rag.consumer.min_idle"},
		{"cleanup_idle == job_timeout", `consumer_cleanup_idle="1h"`, `consumer_cleanup_idle="15m"`, "rag.consumer.consumer_cleanup_idle"},
		{"cleanup_idle < job_timeout", `consumer_cleanup_idle="1h"`, `consumer_cleanup_idle="5m"`, "rag.consumer.consumer_cleanup_idle"},
		{"duration without unit", `job_timeout="15m"`, `job_timeout="15"`, "invalid duration"},
		{"duration not a string", `job_timeout="15m"`, `job_timeout=15`, "invalid duration"},
		{"rag poison_threshold zero", `poison_threshold=3
consumer_cleanup_idle="1h"

[redis]`, `poison_threshold=0
consumer_cleanup_idle="1h"

[redis]`, "rag.consumer.poison_threshold"},
		{"status entry_timeout zero", `entry_timeout="5s"`, `entry_timeout="0s"`, "go.status_consumer.entry_timeout"},
		{"status recovery_interval zero", `recovery_interval="30s"`, `recovery_interval="0s"`, "go.status_consumer.recovery_interval"},
		{"status min_idle zero", `min_idle="60s"`, `min_idle="0s"`, "go.status_consumer.min_idle"},
		{"status poison_threshold zero", `recovery_interval="30s"
poison_threshold=3`, `recovery_interval="30s"
poison_threshold=0`, "go.status_consumer.poison_threshold"},
		{"max_attempts zero", `max_attempts=3`, `max_attempts=0`, "max_attempts"},
		{"no allowed extensions", `allowed_extensions=[".pdf", ".txt", ".md"]`, `allowed_extensions=[]`, "allowed_extensions"},
		{"extension with second dot", `allowed_extensions=[".pdf", ".txt", ".md"]`, `allowed_extensions=[".tar.gz"]`, "allowed_extensions"},
		{"extension with space", `allowed_extensions=[".pdf", ".txt", ".md"]`, `allowed_extensions=[".p df"]`, "allowed_extensions"},
		{"stale rag_grpc section", `[rag_grpc]
port=50051`, `[server]
port=50051`, "rag_grpc.port"},
		{"stale embedding_model key", `embedding_model="qwen3-embedding"`, `model="qwen3-embedding"`, "rag.embedding_model"},
		{"zero call_timeout", `call_timeout="60s"`, `call_timeout="0s"`, "rag.call_timeout"},
		{"compound duration", `job_timeout="15m"`, `job_timeout="14m60s"`, "invalid duration"},
		{"missing redis stream", `stream="ingest.docs"`, `strem="ingest.docs"`, "redis.stream"},
		{"missing redis host", `host="localhost"`, ``, "redis.host"},
		{"zero redis port", `port=6379`, `port=0`, "redis.port"},
		{"zero stream_max_len", `stream_max_len=324000`, `stream_max_len=0`, "redis.stream_max_len"},
		{"missing minio url", `url="localhost:9000"`, ``, "minio.url"},
		{"missing minio access_key", `access_key="minio_user"`, ``, "minio.access_key"},
		{"missing minio secret_key", `secret_key="minio_password"`, ``, "minio.secret_key"},
		{"missing upload_bucket", `upload_bucket="upload-bucket"`, ``, "minio.upload_bucket"},
		{"missing ingestion_bucket", `ingestion_bucket="ingestion-bucket"`, ``, "minio.ingestion_bucket"},
		{"missing go.sqlite_dir", `sqlite_dir="./go_sqlite_db"`, ``, "go.sqlite_dir"},
		{"zero go.port", `port=8080`, `port=0`, "go.port"},
		{"redis port above 65535", `port=6379`, `port=65536`, "redis.port"},
		{"go port above 65535", `port=8080`, `port=65536`, "go.port"},
		{"rag_grpc port above 65535", `port=50051`, `port=65536`, "rag_grpc.port"},
		{"extension without dot", `allowed_extensions=[".pdf", ".txt", ".md"]`, `allowed_extensions=["pdf"]`, "allowed_extensions"},
		{"missing status_stream", `status_stream="ingest.status"`, ``, "status_stream"},
		{"missing status group", `status_consumer_group="center-status"`, ``, "status_consumer_group"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(validTOML, tc.old) {
				t.Fatalf("test bug: %q not in base config", tc.old)
			}
			body := strings.Replace(validTOML, tc.old, tc.new, 1)
			_, err := Load(writeConfig(t, body))
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}

// Both PRD 4.5 timing rules are checked on the shared file, so a drift in either service's
// copy fails at startup instead of reclaiming live jobs.
func TestValidateBoundary(t *testing.T) {
	body := strings.Replace(validTOML, `min_idle="20m"`, `min_idle="1021s"`, 1)
	if _, err := Load(writeConfig(t, body)); err != nil {
		t.Fatalf("min_idle just above job_timeout+grace must be valid: %v", err)
	}
	body = strings.Replace(validTOML, `consumer_cleanup_idle="1h"`, `consumer_cleanup_idle="901s"`, 1)
	if _, err := Load(writeConfig(t, body)); err != nil {
		t.Fatalf("cleanup_idle just above job_timeout must be valid: %v", err)
	}
}

// A file written before [rag.consumer] existed leaves every timing at zero.
func TestLoadRejectsMissingConsumerSection(t *testing.T) {
	start := strings.Index(validTOML, "[rag.consumer]")
	end := strings.Index(validTOML, "[redis]")
	body := validTOML[:start] + validTOML[end:]
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, key := range []string{"rag.consumer.job_timeout", "rag.consumer.min_idle", "rag.consumer.poison_threshold"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error %q does not mention %s", err, key)
		}
	}
}

func TestLoadReportsAllProblemsTogether(t *testing.T) {
	body := strings.Replace(validTOML, `max_attempts=3`, `max_attempts=0`, 1)
	body = strings.Replace(body, `entry_timeout="5s"`, `entry_timeout="0s"`, 1)
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, key := range []string{"ingestion.max_attempts", "go.status_consumer.entry_timeout"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error %q does not mention %s", err, key)
		}
	}
}

// duration_vectors.json is shared with the Python config tests, so both services accept and
// reject exactly the same duration strings.
func TestDurationSharedVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "contracts", "config", "duration_vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Valid []struct {
			Text    string `json:"text"`
			Seconds int64  `json:"seconds"`
		} `json:"valid"`
		Invalid []string `json:"invalid"`
	}
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	if len(vectors.Valid) == 0 || len(vectors.Invalid) == 0 {
		t.Fatal("vector file is empty")
	}
	for _, v := range vectors.Valid {
		var d Duration
		if err := d.UnmarshalText([]byte(v.Text)); err != nil {
			t.Errorf("%q: unexpected error %v", v.Text, err)
			continue
		}
		if got := int64(d.Std() / time.Second); got != v.Seconds {
			t.Errorf("%q = %ds, want %ds", v.Text, got, v.Seconds)
		}
	}
	for _, text := range vectors.Invalid {
		var d Duration
		if err := d.UnmarshalText([]byte(text)); err == nil {
			t.Errorf("%q: expected error, got %v", text, d.Std())
		}
	}
}

// Unknown keys must not fail Load (the Python service adds keys Go does not model), but they
// are reported so a typo is visible.
func TestLoadReportsUnknownKeys(t *testing.T) {
	body := strings.Replace(validTOML, `top_k=5`, "top_k=5\nprompt_template=\"x\"", 1)
	cfg, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("unknown key must not fail Load: %v", err)
	}
	if len(cfg.UnknownKeys) != 1 || cfg.UnknownKeys[0] != "rag.prompt_template" {
		t.Fatalf("UnknownKeys = %v", cfg.UnknownKeys)
	}
	clean, err := Load(writeConfig(t, validTOML))
	if err != nil || len(clean.UnknownKeys) != 0 {
		t.Fatalf("clean config: err=%v unknown=%v", err, clean.UnknownKeys)
	}
}

func TestPort65535IsValid(t *testing.T) {
	body := strings.Replace(validTOML, `port=6379`, `port=65535`, 1)
	if _, err := Load(writeConfig(t, body)); err != nil {
		t.Fatalf("port 65535 must be valid: %v", err)
	}
}

// The compose and e2e files are separate full copies (decision D6), so each must load on its
// own, hold only keys the services model, and keep the timings the e2e depends on.
func TestDeployConfigsLoad(t *testing.T) {
	for _, name := range []string{"config.compose.toml", "config.e2e.toml"} {
		t.Run(name, func(t *testing.T) {
			cfg, err := Load(filepath.Join("..", "..", "..", "..", "deploy", name))
			if err != nil {
				t.Fatal(err)
			}
			if len(cfg.UnknownKeys) != 0 {
				t.Fatalf("unknown keys: %v", cfg.UnknownKeys)
			}
			// Inside the compose network every dependency is reached by service name.
			if cfg.Minio.URL != "minio:9000" || cfg.Redis.Host != "redis" || cfg.Qdrant.URL != "http://qdrant:6333" {
				t.Errorf("hostnames = %s / %s / %s", cfg.Minio.URL, cfg.Redis.Host, cfg.Qdrant.URL)
			}
			if cfg.Ingestion.MaxAttempts != 3 {
				t.Errorf("max_attempts = %d, the PRD acceptance checks assume 3", cfg.Ingestion.MaxAttempts)
			}
		})
	}
}

func TestE2EConfigUsesShortTimings(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "..", "..", "deploy", "config.e2e.toml"))
	if err != nil {
		t.Fatal(err)
	}
	c := cfg.RAG.Consumer
	if c.JobTimeout.Std() != 5*time.Second || c.WatchdogGrace.Std() != 2*time.Second ||
		c.MinIdle.Std() != 10*time.Second || c.RecoveryInterval.Std() != 2*time.Second ||
		c.ConsumerCleanupIdle.Std() != 15*time.Second {
		t.Errorf("python consumer timings = %+v", c)
	}
	// Go's min_idle stays long so an injected pending entry is not reclaimed during the test, and
	// its cleanup is short so the cleanup check does not take a minute.
	s := cfg.Go.StatusConsumer
	if s.MinIdle.Std() != 60*time.Second || s.RecoveryInterval.Std() != 2*time.Second || s.ConsumerCleanupIdle.Std() != 5*time.Second {
		t.Errorf("go status consumer timings = %+v", s)
	}
	// The consumer's publish budget needs a call_timeout that fits inside min_idle.
	if cfg.RAG.CallTimeout.Std() != 5*time.Second {
		t.Errorf("call_timeout = %v", cfg.RAG.CallTimeout.Std())
	}
}
