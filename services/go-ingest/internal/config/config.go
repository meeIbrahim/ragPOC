package config

import "github.com/BurntSushi/toml"

type MinioConfig struct {
	URL       string `toml:"url"`
	AccessKey string `toml:"access_key"`
	SecretKey string `toml:"secret_key"`
	Bucket    string `toml:"bucket"`
}

type RedisConfig struct {
	Host          string `toml:"host"`
	Port          int    `toml:"port"`
	Stream        string `toml:"stream"`
	ConsumerGroup string `toml:"consumer_group"`
}

type GoConfig struct {
	Port      int    `toml:"port"`
	SqliteDir string `toml:"sqlite_dir"`
}

type Config struct {
	Minio MinioConfig `toml:"minio"`
	Redis RedisConfig `toml:"redis"`
	Go    GoConfig    `toml:"go"`
}

func Load(path string) (*Config, error) {
	var cfg Config
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
