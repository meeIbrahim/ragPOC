// Package config loads the shared config.toml settings for the center service.
package config

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Minio     MinioConfig     `toml:"minio"`
	SQLite    SQLiteConfig    `toml:"sqlite"`
	Qdrant    QdrantConfig    `toml:"qdrant"`
	RAGGRPC   RAGGRPCConfig   `toml:"rag_grpc"`
	RAG       RAGConfig       `toml:"rag"`
	Redis     RedisConfig     `toml:"redis"`
	Ingestion IngestionConfig `toml:"ingestion"`
	Go        GoConfig        `toml:"go"`

	// UnknownKeys lists keys in the file that Go does not model, such as ones only the Python
	// service reads. They do not fail Load, so the callers log them to make a typo visible.
	UnknownKeys []string `toml:"-"`
}

// durationPattern is the one duration grammar both services accept: an integer and a single
// unit. It is deliberately narrower than time.ParseDuration (no compound, decimal, negative or
// sub-second values) so the Python reader can match it exactly. The shared test vectors in
// contracts/config/duration_vectors.json pin it.
var durationPattern = regexp.MustCompile(`^[0-9]{1,6}(s|m|h)$`)

// maxPort is the largest TCP port. The Python reader enforces the same bound.
const maxPort = 65535

// Duration is a time.Duration written in config.toml as a string such as "15m".
type Duration time.Duration

func (d *Duration) UnmarshalText(text []byte) error {
	if !durationPattern.Match(text) {
		return fmt.Errorf(`invalid duration %q: want an integer and one unit (s, m or h), e.g. "15m"`, text)
	}
	v, err := time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", text, err)
	}
	*d = Duration(v)
	return nil
}

func (d Duration) Std() time.Duration {
	return time.Duration(d)
}

type MinioConfig struct {
	URL             string `toml:"url"`
	AccessKey       string `toml:"access_key"`
	SecretKey       string `toml:"secret_key"`
	UploadBucket    string `toml:"upload_bucket"`
	IngestionBucket string `toml:"ingestion_bucket"`
}

type SQLiteConfig struct {
	Dir string `toml:"dir"`
}

type QdrantConfig struct {
	URL string `toml:"url"`
}

type RAGGRPCConfig struct {
	Port int `toml:"port"`
}

type RAGConfig struct {
	Collection     string `toml:"collection"`
	EmbeddingModel string `toml:"embedding_model"`
	EmbeddingDim   int    `toml:"embedding_dim"`
	LLMModel       string `toml:"llm_model"`
	TopK           int    `toml:"top_k"`
	ChunkSize      int    `toml:"chunk_size"`
	ChunkOverlap   int    `toml:"chunk_overlap"`
	// CallTimeout bounds each external call the RAG service makes (MinIO, Ollama, Qdrant, Groq).
	CallTimeout Duration          `toml:"call_timeout"`
	Consumer    RAGConsumerConfig `toml:"consumer"`
}

// RAGConsumerConfig is the Python ingest consumer's tuning. Go does not run it but validates
// the shared file at startup, so a bad timing combination fails fast in either service.
type RAGConsumerConfig struct {
	JobTimeout          Duration `toml:"job_timeout"`
	WatchdogGrace       Duration `toml:"watchdog_grace"`
	MinIdle             Duration `toml:"min_idle"`
	RecoveryInterval    Duration `toml:"recovery_interval"`
	PoisonThreshold     int      `toml:"poison_threshold"`
	ConsumerCleanupIdle Duration `toml:"consumer_cleanup_idle"`
}

type RedisConfig struct {
	Host          string `toml:"host"`
	Port          int    `toml:"port"`
	Stream        string `toml:"stream"`
	ConsumerGroup string `toml:"consumer_group"`
	// StreamMaxLen caps the stream length (entries). XADD trims the oldest entries beyond it,
	// whether or not a consumer has read them, so size it above the worst-case backlog.
	StreamMaxLen        int64  `toml:"stream_max_len"`
	StatusStream        string `toml:"status_stream"`
	StatusConsumerGroup string `toml:"status_consumer_group"`
}

type IngestionConfig struct {
	// MaxAttempts is how many times one document is ingested before it is marked FAILED.
	// It is independent of the outbox and hash worker retry limits.
	MaxAttempts       int      `toml:"max_attempts"`
	AllowedExtensions []string `toml:"allowed_extensions"`
}

type GoConfig struct {
	Port           int                  `toml:"port"`
	SQLiteDir      string               `toml:"sqlite_dir"`
	StatusConsumer StatusConsumerConfig `toml:"status_consumer"`
}

type StatusConsumerConfig struct {
	EntryTimeout        Duration `toml:"entry_timeout"`
	MinIdle             Duration `toml:"min_idle"`
	RecoveryInterval    Duration `toml:"recovery_interval"`
	PoisonThreshold     int      `toml:"poison_threshold"`
	ConsumerCleanupIdle Duration `toml:"consumer_cleanup_idle"`
}

// Load reads, parses and validates the TOML config file at path.
func Load(path string) (*Config, error) {
	var cfg Config
	md, err := toml.DecodeFile(path, &cfg)
	if err != nil {
		return nil, fmt.Errorf("config: load %s: %w", path, err)
	}
	for _, key := range md.Undecoded() {
		cfg.UnknownKeys = append(cfg.UnknownKeys, key.String())
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}
	return &cfg, nil
}

// Validate rejects combinations that would make the stream consumers misbehave.
func (c *Config) Validate() error {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	// Keys Go itself reads. A missing or misspelled key decodes to a zero value and would
	// otherwise fail late and vaguely (for example XADD into a stream named "").
	for _, f := range []struct{ name, v string }{
		{"minio.url", c.Minio.URL},
		{"minio.access_key", c.Minio.AccessKey},
		{"minio.secret_key", c.Minio.SecretKey},
		{"minio.upload_bucket", c.Minio.UploadBucket},
		{"minio.ingestion_bucket", c.Minio.IngestionBucket},
		{"redis.host", c.Redis.Host},
		{"redis.stream", c.Redis.Stream},
		{"go.sqlite_dir", c.Go.SQLiteDir},
	} {
		if f.v == "" {
			add("%s must not be empty", f.name)
		}
	}
	if c.Redis.Port < 1 || c.Redis.Port > maxPort {
		add("redis.port must be between 1 and %d", maxPort)
	}
	if c.Redis.StreamMaxLen <= 0 {
		add("redis.stream_max_len must be > 0")
	}

	// Renamed keys ([server] -> [rag_grpc], rag.model -> embedding_model) decode to zero values
	// from a stale file, so require them explicitly.
	if c.RAGGRPC.Port < 1 || c.RAGGRPC.Port > maxPort {
		add("rag_grpc.port must be between 1 and %d", maxPort)
	}
	if c.Go.Port < 1 || c.Go.Port > maxPort {
		add("go.port must be between 1 and %d", maxPort)
	}
	if c.RAG.Collection == "" {
		add("rag.collection must not be empty")
	}
	if c.RAG.EmbeddingModel == "" {
		add("rag.embedding_model must not be empty")
	}
	if c.RAG.CallTimeout <= 0 {
		add("rag.call_timeout must be > 0")
	}

	rc := c.RAG.Consumer
	for _, f := range []struct {
		name string
		d    Duration
	}{
		{"rag.consumer.job_timeout", rc.JobTimeout},
		{"rag.consumer.watchdog_grace", rc.WatchdogGrace},
		{"rag.consumer.min_idle", rc.MinIdle},
		{"rag.consumer.recovery_interval", rc.RecoveryInterval},
		{"rag.consumer.consumer_cleanup_idle", rc.ConsumerCleanupIdle},
	} {
		if f.d <= 0 {
			add("%s must be > 0", f.name)
		}
	}
	if rc.PoisonThreshold < 1 {
		add("rag.consumer.poison_threshold must be >= 1")
	}
	// A live job must never look idle enough to be reclaimed by XAUTOCLAIM.
	if rc.JobTimeout+rc.WatchdogGrace >= rc.MinIdle {
		add("rag.consumer.min_idle (%v) must exceed job_timeout + watchdog_grace (%v)",
			rc.MinIdle.Std(), (rc.JobTimeout + rc.WatchdogGrace).Std())
	}
	// A consumer still running a job must not be deleted as idle.
	if rc.ConsumerCleanupIdle <= rc.JobTimeout {
		add("rag.consumer.consumer_cleanup_idle (%v) must exceed job_timeout (%v)",
			rc.ConsumerCleanupIdle.Std(), rc.JobTimeout.Std())
	}

	sc := c.Go.StatusConsumer
	for _, f := range []struct {
		name string
		d    Duration
	}{
		{"go.status_consumer.entry_timeout", sc.EntryTimeout},
		{"go.status_consumer.min_idle", sc.MinIdle},
		{"go.status_consumer.recovery_interval", sc.RecoveryInterval},
		{"go.status_consumer.consumer_cleanup_idle", sc.ConsumerCleanupIdle},
	} {
		if f.d <= 0 {
			add("%s must be > 0", f.name)
		}
	}
	if sc.PoisonThreshold < 1 {
		add("go.status_consumer.poison_threshold must be >= 1")
	}

	if c.Ingestion.MaxAttempts < 1 {
		add("ingestion.max_attempts must be >= 1")
	}
	if len(c.Ingestion.AllowedExtensions) == 0 {
		add("ingestion.allowed_extensions must not be empty")
	}
	for _, ext := range c.Ingestion.AllowedExtensions {
		// The upload check matches filepath.Ext, which is only the text after the last dot,
		// so ".tar.gz" or an entry with whitespace could never match.
		if !strings.HasPrefix(ext, ".") || len(ext) < 2 || strings.ContainsAny(ext[1:], "./\\ \t") {
			add("ingestion.allowed_extensions entry %q must be one extension such as \".pdf\"", ext)
		}
	}

	if c.Redis.StatusStream == "" {
		add("redis.status_stream must not be empty")
	}
	if c.Redis.StatusConsumerGroup == "" {
		add("redis.status_consumer_group must not be empty")
	}

	if len(problems) > 0 {
		return fmt.Errorf("invalid config: %s", strings.Join(problems, "; "))
	}
	return nil
}
