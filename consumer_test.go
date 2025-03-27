package redis

import (
	"context"
	"log"
	"testing"
	"time"

	redis "github.com/go-redis/redis/v7"
)

func TestConsumer_Subscribe(t *testing.T) {
	{
		/*
			XGROUP CREATE gotestStream1 gotestGroup 0 MKSTREAM
			XGROUP CREATE gotestStream2 gotestGroup 0 MKSTREAM

			XADD gotestStream1 * name luffy age 19
			XADD gotestStream1 * name nami age 21
			XADD gotestStream2 * name roger age ??
			XADD gotestStream2 * name ace age 22

			XGROUP DESTROY gotestStream1 gotestGroup
			XGROUP DESTROY gotestStream2 gotestGroup

			DEL gotestStream1
			DEL gotestStream2
		*/
		client := redis.NewClient(&redis.Options{
			Addr: __TEST_REDIS_SERVER,
			DB:   0,
		})
		if client == nil {
			panic("fail to create redis.Client")
		}
		defer client.Close()

		for _, cmd := range []string{
			"XGROUP CREATE gotestStream1 gotestGroup 0 MKSTREAM",
			"XGROUP CREATE gotestStream2 gotestGroup 0 MKSTREAM",

			"XADD gotestStream1 * name luffy age 19",
			"XADD gotestStream1 * name nami age 21",
			"XADD gotestStream2 * name roger age ??",
			"XADD gotestStream2 * name ace age 22",
		} {
			_, err := execRedisCommand(client, cmd).Result()
			if err != nil {
				panic(err)
			}
		}
		defer func() {
			for _, cmd := range []string{
				"XGROUP DESTROY gotestStream1 gotestGroup",
				"XGROUP DESTROY gotestStream2 gotestGroup",

				"DEL gotestStream1",
				"DEL gotestStream2",
			} {
				_, err := execRedisCommand(client, cmd).Result()
				if err != nil {
					panic(err)
				}
			}
		}()
	}

	opt := redis.UniversalOptions{
		Addrs: []string{__TEST_REDIS_SERVER},
		DB:    0,
	}

	var msgCnt int = 0

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

	c := &Consumer{
		Group:               "gotestGroup",
		Name:                "gotestConsumer",
		RedisOption:         &opt,
		MaxInFlight:         8,
		MaxPollingTimeout:   10 * time.Millisecond,
		ClaimMinIdleTime:    5 * time.Second,
		IdlingTimeout:       10 * time.Second,
		ClaimSensitivity:    8,
		ClaimOccurrenceRate: 1,
		MessageHandler: func(message *Message) {
			log.Printf("Stream: %s, Message:%+v\n", message.Stream, message)
			msgCnt++
			message.Ack()
			message.Del()
		},
	}

	err := c.Subscribe(
		Stream("gotestStream1").NeverDeliveredOffset(),
		Stream("gotestStream2").NeverDeliveredOffset(),
	)
	if err != nil {
		t.Fatal(err)
	}

	<-ctx.Done()
	c.Close()

	// assert
	{
		var expectedMsgCnt int = 4
		if msgCnt != expectedMsgCnt {
			t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
		}
	}
}

func TestConsumer_Subscribe_WithStreamFilter(t *testing.T) {
	{
		/*
			XGROUP CREATE gotestStream1 gotestGroup 0 MKSTREAM
			XGROUP CREATE gotestStream2 gotestGroup 0 MKSTREAM

			XADD gotestStream1 * name luffy age 19
			XADD gotestStream1 * name nami age 21
			XADD gotestStream2 * name roger age ??
			XADD gotestStream2 * name ace age 22

			XGROUP DESTROY gotestStream1 gotestGroup
			XGROUP DESTROY gotestStream2 gotestGroup

			DEL gotestStream1
			DEL gotestStream2
		*/
		client := redis.NewClient(&redis.Options{
			Addr: __TEST_REDIS_SERVER,
			DB:   0,
		})
		if client == nil {
			panic("fail to create redis.Client")
		}
		defer client.Close()

		for _, cmd := range []string{
			"XGROUP CREATE gotestStream1 gotestGroup 0 MKSTREAM",
			"XGROUP CREATE gotestStream2 gotestGroup 0 MKSTREAM",

			"XADD gotestStream1 * name luffy age 19",
			"XADD gotestStream1 * name nami age 21",
			"XADD gotestStream2 * name roger age ??",
			"XADD gotestStream2 * name ace age 22",
		} {
			_, err := execRedisCommand(client, cmd).Result()
			if err != nil {
				panic(err)
			}
		}
		defer func() {
			for _, cmd := range []string{
				"XGROUP DESTROY gotestStream1 gotestGroup",
				"XGROUP DESTROY gotestStream2 gotestGroup",

				"DEL gotestStream1",
				"DEL gotestStream2",
			} {
				_, err := execRedisCommand(client, cmd).Result()
				if err != nil {
					panic(err)
				}
			}
		}()
	}

	opt := redis.UniversalOptions{
		Addrs: []string{__TEST_REDIS_SERVER},
		DB:    0,
	}

	var msgCnt int = 0

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

	c := &Consumer{
		Group:               "gotestGroup",
		Name:                "gotestConsumer",
		RedisOption:         &opt,
		MaxInFlight:         8,
		MaxPollingTimeout:   10 * time.Millisecond,
		ClaimMinIdleTime:    5 * time.Second,
		IdlingTimeout:       10 * time.Second,
		ClaimSensitivity:    8,
		ClaimOccurrenceRate: 1,
		MessageHandler: func(message *Message) {
			log.Printf("Stream: %s, Message:%+v\n", message.Stream, message)
			msgCnt++
			message.Ack()
			message.Del()
		},
		StreamFilter: func(message *Message) bool {
			if message.Values != nil {
				if message.Values["name"] == "luffy" {
					log.Printf("[StreamFilter]:: Stream: %s, Message:%+v\n", message.Stream, message)
					return false
				}
			}
			return true
		},
	}

	err := c.Subscribe(
		Stream("gotestStream1").NeverDeliveredOffset(),
		Stream("gotestStream2").NeverDeliveredOffset(),
	)
	if err != nil {
		t.Fatal(err)
	}

	<-ctx.Done()
	c.Close()

	// assert
	{
		var expectedMsgCnt int = 3
		if msgCnt != expectedMsgCnt {
			t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
		}
	}
}
