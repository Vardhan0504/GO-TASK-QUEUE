package queue

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Queue keys and Redis Pub/Sub channel constants
const (
	QueuePending    = "queue:pending"
	QueueProcessing = "queue:processing"
	QueueDelayed    = "queue:delayed"
	QueueDLQ        = "queue:dlq"
	ChannelUpdates  = "queue:events"
)

type RedisClient struct {
	Client *redis.Client
}

// NewRedisClient initializes connection to Redis (supports standard addrs and rediss:// TLS URLs)
func NewRedisClient(redisURL string) (*RedisClient, error) {
	var opts *redis.Options
	var err error

	if strings.HasPrefix(redisURL, "rediss://") || strings.HasPrefix(redisURL, "redis://") {
		opts, err = redis.ParseURL(redisURL)
		if err != nil {
			return nil, err
		}
	} else {
		opts = &redis.Options{
			Addr: redisURL,
		}
	}

	// Enable TLS verification skip for cloud providers like Upstash if needed
	if strings.HasPrefix(redisURL, "rediss://") {
		opts.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}

	rdb := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &RedisClient{Client: rdb}, nil
}

// Lua script to move due tasks from delayed ZSET to pending LIST atomically
var pollDelayedLua = redis.NewScript(`
local due = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, 50)
if #due > 0 then
    for _, task in ipairs(due) do
        redis.call('RPUSH', KEYS[2], task)
        redis.call('ZREM', KEYS[1], task)
    end
end
return #due
`)

// PollDelayedTasks executes the atomic Lua script to promote ready retry tasks
func (r *RedisClient) PollDelayedTasks(ctx context.Context) (int64, error) {
	now := time.Now().Unix()
	res, err := pollDelayedLua.Run(ctx, r.Client, []string{QueueDelayed, QueuePending}, now).Result()
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}