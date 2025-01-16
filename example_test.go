package redis_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	redis "github.com/Bofry/lib-redis-stream"
)

func Example() {
	var (
		EVN_REDIS_SERVERS = strings.Split(os.Getenv("REDIS_SERVERS"), ",")
	)
	if len(EVN_REDIS_SERVERS) == 0 {
		EVN_REDIS_SERVERS = []string{"127.0.0.1:6379"}
	}

	// register consumer group
	{
		admin, err := redis.NewAdminClient(&redis.UniversalOptions{
			Addrs: EVN_REDIS_SERVERS,
			DB:    0,
		})
		if err != nil {
			panic(err)
		}
		defer func() {
			defer admin.Close()

			count, err := admin.Handle().XLen("gotestStream").Result()
			if err != nil {
				panic(err)
			}
			fmt.Printf("Retained messages: %d\n", count)

			/*
				XGROUP DESTROY gotestStream gotestGroup
			*/
			_, err = admin.DeleteConsumerGroup("gotestStream", "gotestGroup")
			if err != nil {
				panic(err)
			}

			/*
				DEL gotestStream
			*/
			_, err = admin.Handle().Del("gotestStream").Result()
			if err != nil {
				panic(err)
			}
		}()

		/*
			XGROUP CREATE gotestStream gotestGroup $ MKSTREAM
		*/
		_, err = admin.CreateConsumerGroupAndStream("gotestStream", "gotestGroup", redis.StreamLastDeliveredID)
		if err != nil {
			panic(err)
		}
	}

	// publish
	{
		conf := redis.ProducerConfig{
			UniversalOptions: &redis.UniversalOptions{
				Addrs: EVN_REDIS_SERVERS,
				DB:    0,
			},
		}
		p, err := redis.NewProducer(&conf)
		if err != nil {
			panic(err)
		}
		defer p.Close()

		// produce message
		{
			publishMessages := []struct {
				id     string
				values map[string]interface{}
			}{
				{id: "4567-0", values: map[string]interface{}{"name": "luffy", "age": 19}},
				{id: "4567-1", values: map[string]interface{}{"name": "nami", "age": 21}},
				{id: "4567-2", values: map[string]interface{}{"name": "zoro", "age": 21}},
			}

			for _, message := range publishMessages {
				reply, err := p.Write("gotestStream", message.values, redis.WithMessageID(message.id))
				if err != nil {
					panic(err)
				}
				_ = reply
				fmt.Printf("ID: %s\n", reply)
			}
		}
	}

	// subscribe
	{
		// the config only for test use !!
		opt := redis.UniversalOptions{
			Addrs: EVN_REDIS_SERVERS,
			DB:    0,
		}

		c := &redis.Consumer{
			Group:               "gotestGroup",
			Name:                "gotestConsumer",
			RedisOption:         &opt,
			MaxInFlight:         1,
			MaxPollingTimeout:   10 * time.Millisecond,
			ClaimMinIdleTime:    30 * time.Millisecond,
			IdlingTimeout:       2000 * time.Millisecond,
			ClaimSensitivity:    2,
			ClaimOccurrenceRate: 2,
			MessageHandler: func(message *redis.Message) {
				fmt.Printf("Message on %s: %v\n", message.Stream, message.XMessage)
				message.Ack()
				message.Del()
			},
			ErrorHandler: func(err error) (disposed bool) {
				fmt.Println(err)
				return true
			},
		}

		err := c.Subscribe(
			redis.Stream("gotestStream"),
		)
		if err != nil {
			panic(err)
		}

		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

		select {
		case <-ctx.Done():
			c.Close()
			break
		}
	}

	// Output:
	// ID: 4567-0
	// ID: 4567-1
	// ID: 4567-2
	// Message on gotestStream: &{4567-0 map[age:19 name:luffy]}
	// Message on gotestStream: &{4567-1 map[age:21 name:nami]}
	// Message on gotestStream: &{4567-2 map[age:21 name:zoro]}
	// Retained messages: 0
}
