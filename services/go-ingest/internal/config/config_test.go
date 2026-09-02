package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadParsesMinioRedisAndGoSections(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	contents := `
[minio]
url="localhost:9000"
access_key="minio_user"
secret_key="minio_password"
bucket="rag-pdf-source"

[redis]
host="localhost"
port=6379
stream="ingest.docs"
consumer_group="rag-pdf-fixed"

[go]
port=8080
sqlite_dir="./go_sqlite_db"
`
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.Minio.Bucket != "rag-pdf-source" {
		t.Errorf("Minio.Bucket = %q, want rag-pdf-source", cfg.Minio.Bucket)
	}
	if cfg.Redis.Port != 6379 {
		t.Errorf("Redis.Port = %d, want 6379", cfg.Redis.Port)
	}
	if cfg.Go.Port != 8080 {
		t.Errorf("Go.Port = %d, want 8080", cfg.Go.Port)
	}
}
