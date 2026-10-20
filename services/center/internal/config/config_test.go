package config

import (
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
	if _, err := Load(filepath.Join("..", "..", "..", "..", "config.toml")); err != nil {
		t.Fatalf("repo config.toml: %v", err)
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
		{"job_timeout + grace == min_idle", `min_idle="20m"`, `min_idle="17m"`, "min_idle"},
		{"job_timeout + grace > min_idle", `min_idle="20m"`, `min_idle="10m"`, "min_idle"},
		{"cleanup_idle == job_timeout", `consumer_cleanup_idle="1h"`, `consumer_cleanup_idle="15m"`, "consumer_cleanup_idle"},
		{"cleanup_idle < job_timeout", `consumer_cleanup_idle="1h"`, `consumer_cleanup_idle="5m"`, "consumer_cleanup_idle"},
		{"duration without unit", `job_timeout="15m"`, `job_timeout="15"`, "job_timeout"},
		{"duration not a string", `job_timeout="15m"`, `job_timeout=15`, "job_timeout"},
		{"rag poison_threshold zero", `poison_threshold=3
consumer_cleanup_idle="1h"

[redis]`, `poison_threshold=0
consumer_cleanup_idle="1h"

[redis]`, "poison_threshold"},
		{"status entry_timeout zero", `entry_timeout="5s"`, `entry_timeout="0s"`, "entry_timeout"},
		{"status recovery_interval zero", `recovery_interval="30s"`, `recovery_interval="0s"`, "recovery_interval"},
		{"status min_idle zero", `min_idle="60s"`, `min_idle="0s"`, "min_idle"},
		{"status poison_threshold zero", `recovery_interval="30s"
poison_threshold=3`, `recovery_interval="30s"
poison_threshold=0`, "poison_threshold"},
		{"max_attempts zero", `max_attempts=3`, `max_attempts=0`, "max_attempts"},
		{"no allowed extensions", `allowed_extensions=[".pdf", ".txt", ".md"]`, `allowed_extensions=[]`, "allowed_extensions"},
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
	body := strings.Replace(validTOML, `min_idle="20m"`, `min_idle="17m1s"`, 1)
	if _, err := Load(writeConfig(t, body)); err != nil {
		t.Fatalf("min_idle just above job_timeout+grace must be valid: %v", err)
	}
	body = strings.Replace(validTOML, `consumer_cleanup_idle="1h"`, `consumer_cleanup_idle="15m1s"`, 1)
	if _, err := Load(writeConfig(t, body)); err != nil {
		t.Fatalf("cleanup_idle just above job_timeout must be valid: %v", err)
	}
}
