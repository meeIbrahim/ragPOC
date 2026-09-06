// Package config loads the shared config.toml settings for the center service.
package config

import (
	"fmt"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Minio  MinioConfig  `toml:"minio"`
	SQLite SQLiteConfig `toml:"sqlite"`
	Qdrant QdrantConfig `toml:"qdrant"`
	Server ServerConfig `toml:"server"`
	RAG    RAGConfig    `toml:"rag"`
	Redis  RedisConfig  `toml:"redis"`
	Go     GoConfig     `toml:"go"`
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

type ServerConfig struct {
	Port int `toml:"port"`
}

type RAGConfig struct {
	Collection string `toml:"collection"`
	Model      string `toml:"model"`
}

type RedisConfig struct {
	Host          string `toml:"host"`
	Port          int    `toml:"port"`
	Stream        string `toml:"stream"`
	ConsumerGroup string `toml:"consumer_group"`
}

type GoConfig struct {
	Port      int    `toml:"port"`
	SQLiteDir string `toml:"sqlite_dir"`
}

// Load reads and parses the TOML config file at path.
func Load(path string) (*Config, error) {
	var cfg Config
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return nil, fmt.Errorf("config: load %s: %w", path, err)
	}
	return &cfg, nil
}
