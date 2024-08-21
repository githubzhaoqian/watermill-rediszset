# Watermill Redis Stream Pub/Sub
<img align="right" width="200" src="https://threedots.tech/watermill-io/watermill-logo.png">

This is Pub/Sub for the [Watermill](https://watermill.io/) project.

All Pub/Sub implementations can be found at [https://watermill.io/pubsubs/](https://watermill.io/pubsubs/).

Watermill is a Go library for working efficiently with message streams. It is intended
for building event driven applications, enabling event sourcing, RPC over messages,
sagas and basically whatever else comes to your mind. You can use conventional pub/sub
implementations like Kafka or RabbitMQ, but also HTTP or MySQL binlog if that fits your use case.

Documentation: https://watermill.io/

Getting started guide: https://watermill.io/docs/getting-started/

Issues: https://github.com/ThreeDotsLabs/watermill/issues

## Contributing

All contributions are very much welcome. If you'd like to help with Watermill development,
please see [open issues](https://github.com/ThreeDotsLabs/watermill/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+)
and submit your pull request via GitHub.

## Support

If you didn't find the answer to your question in [the documentation](https://watermill.io/), feel free to ask us directly!

Please join us on the `#watermill` channel on the [Gophers slack](https://gophers.slack.com/): You can get an invite [here](https://gophersinvite.herokuapp.com/).

## License

[MIT License](./LICENSE)


Exzample:
```golang
package main

import (
	"context"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"

	"github.com/githubzhaoqian/watermill-rediszset/pkg/rediszset"
)

func main() {
	subClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})
	subscriber, err := rediszset.NewSubscriber(
		rediszset.SubscriberConfig{
			Client:       subClient,
			Unmarshaller: rediszset.DefaultMarshallerUnmarshaller{},
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	messages, err := subscriber.Subscribe(context.Background(), "example.topic")
	if err != nil {
		panic(err)
	}

	go process(messages)

	pubClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})
	publisher, err := rediszset.NewPublisher(
		rediszset.PublisherConfig{
			Client:     pubClient,
			Marshaller: rediszset.DefaultMarshallerUnmarshaller{},
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	publishMessages(publisher)
}

func publishMessages(publisher message.Publisher) {
	for {
		msg := message.NewMessage(watermill.NewUUID(), []byte("Hello, world!"))
		msg.Metadata.Set(rediszset.DelayKey, "100")
		if err := publisher.Publish("example.topic", msg); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}

func process(messages <-chan *message.Message) {
	for msg := range messages {
		log.Printf("received message: %s, payload: %s", msg.UUID, string(msg.Payload))
		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
}

```
