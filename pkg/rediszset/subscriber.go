package rediszset

import (
	"context"
	"sync"
	"time"

	"github.com/Rican7/retry"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

const (
	// NoSleep can be set to SubscriberConfig.NackResendSleep
	NoSleep time.Duration = -1

	DefaultBlockTime = time.Millisecond * 100

	DefaultClaimInterval = time.Second * 5

	DefaultClaimBatchSize = int64(100)

	DefaultMaxIdleTime = time.Second * 60

	DefaultCheckConsumersInterval = time.Second * 300
	DefaultConsumerTimeout        = time.Second * 600
	DefaultConsumerLockExpiration = 300
	DefaultZRangeCount            = 100
)

type Subscriber struct {
	config        SubscriberConfig
	client        redis.UniversalClient
	logger        watermill.LoggerAdapter
	closing       chan struct{}
	subscribersWg sync.WaitGroup

	closed     bool
	closeMutex sync.Mutex
}

// NewSubscriber creates a new redis stream Subscriber.
func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = &watermill.NopLogger{}
	}

	return &Subscriber{
		config:  config,
		client:  config.Client,
		logger:  logger,
		closing: make(chan struct{}),
	}, nil
}

type SubscriberConfig struct {
	Client redis.UniversalClient

	Unmarshaller Unmarshaller

	// Redis stream consumer id, paired with ConsumerGroup.
	Consumer string

	// How long after Nack message should be redelivered.
	NackResendSleep time.Duration

	// Block to wait next redis stream message.
	BlockTime time.Duration

	// Claim idle pending message interval.
	ClaimInterval time.Duration

	// How many pending messages are claimed at most each claim interval.
	ClaimBatchSize int64

	// How long should we treat a pending message as claimable.
	MaxIdleTime time.Duration

	// Check consumer status interval.
	CheckConsumersInterval time.Duration

	// After this timeout an idle consumer with no pending messages will be removed from the consumer group.
	ConsumerTimeout time.Duration

	// Start consumption from the specified message ID.
	// When using "0", the consumer group will consume from the very first message.
	// When using "$", the consumer group will consume from the latest message.
	OldestId string

	// If consumer group in not set, for fanout start consumption from the specified message ID.
	// When using "0", the consumer will consume from the very first message.
	// When using "$", the consumer will consume from the latest message.
	FanOutOldestId string

	// If this is set, it will be called to decide whether a pending message that
	// has been idle for more than MaxIdleTime should actually be claimed.
	// If this is not set, then all pending messages that have been idle for more than MaxIdleTime will be claimed.
	// This can be useful e.g. for tasks where the processing time can be very variable -
	// so we can't just use a short MaxIdleTime; but at the same time dead
	// consumers should be spotted quickly - so we can't just use a long MaxIdleTime either.
	// In such cases, if we have another way for checking consumers' health, then we can
	// leverage that in this callback.
	ShouldClaimPendingMessage func(redis.XPendingExt) bool

	// If this is set, it will be called to decide whether a reading error
	// should return the read method and close the subscriber or just log the error
	// and continue.
	ShouldStopOnReadErrors func(error) bool

	LockExpiration int64

	ZRangeCount int64
}

func (sc *SubscriberConfig) setDefaults() {
	if sc.Unmarshaller == nil {
		sc.Unmarshaller = DefaultMarshallerUnmarshaller{}
	}
	if sc.Consumer == "" {
		sc.Consumer = watermill.NewShortUUID()
	}
	if sc.NackResendSleep == 0 {
		sc.NackResendSleep = NoSleep
	}
	if sc.BlockTime == 0 {
		sc.BlockTime = DefaultBlockTime
	}
	if sc.ClaimInterval == 0 {
		sc.ClaimInterval = DefaultClaimInterval
	}
	if sc.ClaimBatchSize == 0 {
		sc.ClaimBatchSize = DefaultClaimBatchSize
	}
	if sc.MaxIdleTime == 0 {
		sc.MaxIdleTime = DefaultMaxIdleTime
	}
	if sc.CheckConsumersInterval == 0 {
		sc.CheckConsumersInterval = DefaultCheckConsumersInterval
	}
	if sc.ConsumerTimeout == 0 {
		sc.ConsumerTimeout = DefaultConsumerTimeout
	}
	// Consume from scratch by default
	if sc.OldestId == "" {
		sc.OldestId = "0"
	}

	if sc.FanOutOldestId == "" {
		sc.FanOutOldestId = "$"
	}

	if sc.LockExpiration == 0 {
		sc.LockExpiration = DefaultConsumerLockExpiration
	}

	if sc.ZRangeCount == 0 {
		sc.ZRangeCount = DefaultZRangeCount
	}
}

func (sc *SubscriberConfig) Validate() error {
	if sc.Client == nil {
		return errors.New("redis client is empty")
	}
	return nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	s.subscribersWg.Add(1)

	logFields := watermill.LogFields{
		"provider":      "redis",
		"topic":         topic,
		"consumer_uuid": s.config.Consumer,
	}
	s.logger.Info("Subscribing to redis stream topic", logFields)

	// we don't want to have buffered channel to not consume messsage from redis stream when consumer is not consuming
	output := make(chan *message.Message)

	consumeClosed, err := s.consumeMessages(ctx, topic, output, logFields)
	if err != nil {
		s.subscribersWg.Done()
		return nil, err
	}

	go func() {
		<-consumeClosed
		close(output)
		s.subscribersWg.Done()
	}()

	return output, nil
}

func (s *Subscriber) consumeMessages(ctx context.Context, topic string, output chan *message.Message, logFields watermill.LogFields) (consumeMessageClosed chan struct{}, err error) {
	s.logger.Info("Starting consuming", logFields)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-s.closing:
			s.logger.Debug("Closing subscriber, cancelling consumeMessages", logFields)
			cancel()
		case <-ctx.Done():
			// avoid goroutine leak
		}
	}()

	consumeMessageClosed, err = s.consumeMessage(ctx, topic, output, logFields)
	if err != nil {
		s.logger.Debug(
			"Starting consume failed, cancelling context",
			logFields.Add(watermill.LogFields{"err": err}),
		)
		cancel()
		return nil, err
	}

	return consumeMessageClosed, nil
}

func (s *Subscriber) consumeMessage(ctx context.Context, topic string, output chan *message.Message, logFields watermill.LogFields) (chan struct{}, error) {
	messageHandler := s.createMessageHandler(output)
	consumeMessageClosed := make(chan struct{})

	go func() {
		defer close(consumeMessageClosed)

		readChannel := make(chan *ReadChannel, 1)
		go s.read(ctx, topic, readChannel, logFields)

		for {
			select {
			case rz := <-readChannel:
				if rz == nil {
					s.logger.Debug("readStreamChannel is closed, stopping readStream", logFields)
					return
				}
				if err := messageHandler.processMessage(ctx, topic, rz, logFields); err != nil {
					s.logger.Error("processMessage fail", err, logFields)
					return
				}
			case <-s.closing:
				s.logger.Debug("Subscriber is closing, stopping readStream", logFields)
				return
			case <-ctx.Done():
				s.logger.Debug("Ctx was cancelled, stopping readStream", logFields)
				return
			}
		}
	}()

	return consumeMessageClosed, nil
}

func (s *Subscriber) read(ctx context.Context, topic string, readChannel chan<- *ReadChannel, logFields watermill.LogFields) {
	wg := &sync.WaitGroup{}
	defer func() {
		wg.Wait()
		close(readChannel)
	}()

	for {
		var (
			rzs  []redis.Z
			rz   *redis.Z
			lock *Lock
			err  error
		)
		select {
		case <-s.closing:
			return
		case <-ctx.Done():
			return
		default:
			// 获取多个
			rzs, err = s.client.ZRangeArgsWithScores(
				ctx,
				redis.ZRangeArgs{
					Key:     topic,
					Start:   0,
					Stop:    time.Now().Unix(),
					ByScore: true,
					Offset:  0,
					Count:   s.config.ZRangeCount,
				}).Result()
			if err == redis.Nil {
				continue
			} else if err != nil {
				if s.config.ShouldStopOnReadErrors != nil {
					if s.config.ShouldStopOnReadErrors(err) {
						s.logger.Error("stop reading after error", err, logFields)
						return
					}
				}
				// prevent excessive output from abnormal connections
				time.Sleep(500 * time.Millisecond)
				s.logger.Error("read fail", err, logFields)
			}
			if len(rzs) < 1 || len(rzs[0].Member.(string)) < 1 {
				continue
			}
			for _, item := range rzs {
				id := item.Member.(string)
				tmpLock, ok, err := NewLock(ctx, s.client, getLockKey(topic, id), s.config.LockExpiration)
				if err != nil {
					s.logger.Error("lock fail", err, logFields)
					continue
				}
				if !ok {
					continue
				}
				lock = tmpLock
				// update last delivered message
				rz = &redis.Z{
					Score:  item.Score,
					Member: item.Member,
				}
			}
			if rz == nil || len(rz.Member.(string)) < 1 {
				continue
			}
			select {
			case <-s.closing:
				return
			case <-ctx.Done():
				return
			case readChannel <- &ReadChannel{
				Z:    rz,
				Lock: lock,
			}:
			}
		}
	}
}

func (s *Subscriber) createMessageHandler(output chan *message.Message) messageHandler {
	return messageHandler{
		outputChannel:   output,
		rc:              s.client,
		unmarshaller:    s.config.Unmarshaller,
		nackResendSleep: s.config.NackResendSleep,
		logger:          s.logger,
		closing:         s.closing,
	}
}

func (s *Subscriber) Close() error {
	s.closeMutex.Lock()
	defer s.closeMutex.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	close(s.closing)
	s.subscribersWg.Wait()

	if err := s.client.Close(); err != nil {
		return err
	}

	s.logger.Debug("Redis stream subscriber closed", nil)

	return nil
}

type messageHandler struct {
	outputChannel chan<- *message.Message
	rc            redis.UniversalClient
	unmarshaller  Unmarshaller

	nackResendSleep time.Duration

	logger  watermill.LoggerAdapter
	closing chan struct{}
}

func (h *messageHandler) processMessage(ctx context.Context, topic string, read *ReadChannel, messageLogFields watermill.LogFields) error {
	receivedMsgLogFields := messageLogFields.Add(watermill.LogFields{
		"score": read.Z.Score,
	})
	h.logger.Trace("Received message from redis zset", receivedMsgLogFields)
	src := read.Z.Member.(string)
	srcKey := getKey(topic, src)
	member, err := h.rc.Get(ctx, srcKey).Result()
	if errors.Is(err, redis.Nil) {
		h.logger.Error("Message nil", err, watermill.LogFields{"key": srcKey})
		return nil
	}
	if err != nil {
		return errors.Wrapf(err, "message %s get failed", srcKey)
	}
	msg, err := h.unmarshaller.Unmarshal(member)
	if err != nil {
		return errors.Wrapf(err, "message unmarshal failed")
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	receivedMsgLogFields = receivedMsgLogFields.Add(watermill.LogFields{
		"message_uuid": msg.UUID,
		"topic":        topic,
		"score":        read.Z.Score,
	})

ResendLoop:
	for {
		select {
		case h.outputChannel <- msg:
			h.logger.Trace("Message sent to consumer", receivedMsgLogFields)
		case <-h.closing:
			h.logger.Trace("Closing, message discarded", receivedMsgLogFields)
			return nil
		case <-ctx.Done():
			h.logger.Trace("Closing, ctx cancelled before sent to consumer", receivedMsgLogFields)
			return nil
		}

		select {
		case <-msg.Acked():
			// deadly retry ack
			err := retry.Retry(func(attempt uint) error {
				// 删除 zset 和 string
				err = h.rc.ZRem(ctx, topic, msg.UUID).Err()
				if err != nil {
					return errors.WithStack(err)
				}
				delKey := getKey(topic, msg.UUID)
				err = h.rc.Del(ctx, delKey).Err()
				if err != nil {
					return errors.WithStack(err)
				}
				// 解锁
				_ = read.Lock.Close(ctx)
				return nil
			}, func(attempt uint) bool {
				if attempt != 0 {
					time.Sleep(time.Millisecond * 100)
				}
				return true
			}, func(attempt uint) bool {
				select {
				case <-h.closing:
				case <-ctx.Done():
				default:
					return true
				}
				return false
			})
			if err != nil {
				h.logger.Error("Message Acked fail", err, receivedMsgLogFields)
			}
			h.logger.Trace("Message Acked", receivedMsgLogFields)
			break ResendLoop
		case <-msg.Nacked():
			h.logger.Trace("Message Nacked", receivedMsgLogFields)

			// reset acks, etc.
			msg = msg.Copy()
			if h.nackResendSleep != NoSleep {
				time.Sleep(h.nackResendSleep)
			}

			continue ResendLoop
		case <-h.closing:
			h.logger.Trace("Closing, message discarded before ack", receivedMsgLogFields)
			return nil
		case <-ctx.Done():
			h.logger.Trace("Closing, ctx cancelled before ack", receivedMsgLogFields)
			return nil
		}
	}

	return nil
}
