package rediszset

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

type Member struct {
	Topic string  `json:"topic"`
	UUID  string  `json:"uuid"`
	Score float64 `json:"score"`

	// Metadata contains the message metadata.
	//
	// Can be used to store data which doesn't require unmarshalling the entire payload.
	// It is something similar to HTTP request's headers.
	//
	// Metadata is marshaled and will be saved to the PubSub.
	Metadata message.Metadata `json:"metadata"`

	// Payload is the message's payload.
	Payload message.Payload `json:"payload"`
}

type ReadChannel struct {
	Z    *redis.Z
	Lock *Lock
}

type Lock struct {
	key    string
	uuid   string
	client redis.UniversalClient
}

func NewLock(ctx context.Context, client redis.UniversalClient, lockKey string, seconds int64) (*Lock, bool, error) {
	id := uuid.NewString()
	lock, err := client.SetNX(ctx, lockKey, id, time.Duration(seconds)*time.Second).Result()
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	if !lock {
		return nil, false, nil
	}
	return &Lock{
		uuid:   id,
		key:    lockKey,
		client: client,
	}, true, nil
}

func (l *Lock) Close(ctx context.Context) error {
	lockVal, err := l.client.Get(ctx, l.key).Result()
	if err != nil {
		return errors.WithStack(err)
	}
	if lockVal != l.uuid {
		return nil
	}
	_, err = l.client.Del(ctx, l.key).Result()
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
