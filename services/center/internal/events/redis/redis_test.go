package redis

import (
	"context"
	"net"
	"strings"
	"testing"

	"center-service/internal/config"
)

// A center that cannot reach Redis at startup must fail clearly instead of running without
// its producer and status consumer.
func TestNewClientFailsWhenRedisIsUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close() // nothing listens on port now

	c, err := NewClient(context.Background(), config.RedisConfig{Host: "127.0.0.1", Port: port, Stream: "s"})
	if err == nil || c != nil {
		t.Fatalf("NewClient = %v, %v; want nil and an error", c, err)
	}
	if !strings.Contains(err.Error(), "connect to redis") {
		t.Errorf("err = %v; want it to say the Redis connection failed", err)
	}
}
