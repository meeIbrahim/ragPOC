package redis

import (
	"center-service/internal/events"
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type Producer struct {
	client *Client
}

func NewProducer(client *Client) *Producer {
	return &Producer{
		client: client,
	}
}

func (p *Producer) PublishIngestionEvent(ctx context.Context, event *events.StreamIngestionEvent) (string, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf("failed to marshal event: %w", err)
	}
	args := &redis.XAddArgs{
		Stream: p.client.IngestionStream,
		ID:     "*",
		Values: map[string]any{
			"event_type": "IngestionRequest",
			"payload":    string(data),
		},
		MaxLen: p.client.StreamMaxLen,
		Approx: true,
	}
	messageID, err := p.client.rdb.XAdd(ctx, args).Result()
	if err != nil {
		return "", fmt.Errorf("failed to publish ingestion event: %w", err)
	}
	return messageID, nil
}
