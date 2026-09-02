package stream

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Publisher interface {
	Publish(ctx context.Context, hashID, objectPath, contentType string) error
}

type RedisPublisher struct {
	client *redis.Client
	stream string
}

func NewRedisPublisher(addr, streamName string) *RedisPublisher {
	return &RedisPublisher{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		stream: streamName,
	}
}

func (p *RedisPublisher) Publish(ctx context.Context, hashID, objectPath, contentType string) error {
	return p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: p.stream,
		Values: map[string]interface{}{
			"hash_id":      hashID,
			"object_path":  objectPath,
			"content_type": contentType,
		},
	}).Err()
}
