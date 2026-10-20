package redis

import (
	"center-service/internal/config"
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb             *redis.Client
	IngestionStream string
	StreamMaxLen    int64
	// StatusStream and StatusGroup are the stream Python publishes ingest status to and the
	// consumer group Go reads it with.
	StatusStream string
	StatusGroup  string
}

func NewClient(ctx context.Context, cfg config.RedisConfig) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
	})
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		rdb.Close()
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}
	return &Client{
		rdb:             rdb,
		IngestionStream: cfg.Stream,
		StreamMaxLen:    cfg.StreamMaxLen,
		StatusStream:    cfg.StatusStream,
		StatusGroup:     cfg.StatusConsumerGroup,
	}, nil
}

func (c *Client) Close() error {
	return c.rdb.Close()
}
