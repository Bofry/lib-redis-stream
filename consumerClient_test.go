package redis

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	redis "github.com/go-redis/redis/v7"
	"github.com/joho/godotenv"
)

var (
	__TEST_REDIS_SERVER string

	__ENV_FILE        = ".env"
	__ENV_FILE_SAMPLE = ".env.sample"
)

func copyFile(src, dst string) error {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destination.Close()
	_, err = io.Copy(destination, source)
	return err
}

func execRedisCommand(client *redis.Client, cmd string) *redis.Cmd {
	var args []interface{}
	for _, argv := range strings.Split(cmd, " ") {
		args = append(args, argv)
	}
	return client.Do(args...)
}

func init() {
	_, err := os.Stat(__ENV_FILE)
	if err != nil {
		if os.IsNotExist(err) {
			err = copyFile(__ENV_FILE_SAMPLE, __ENV_FILE)
			if err != nil {
				panic(err)
			}
		}
	}

	{
		f, err := os.Open(__ENV_FILE)
		if err != nil {
			panic(err)
		}
		env, err := godotenv.Parse(f)
		if err != nil {
			panic(err)
		}
		__TEST_REDIS_SERVER = env["TEST_REDIS_SERVER"]
	}
}

func TestConsumerClient_Read(t *testing.T) {
	{
		/*
			XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM
			XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM

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
			"XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM",
			"XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM",

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

	c := &ConsumerClient{
		Group:       "gotestGroup",
		Name:        "gotestConsumer",
		RedisOption: &opt,
	}

	err := c.subscribe(
		StreamOffset{Stream: "gotestStream1", Offset: StreamNeverDeliveredOffset},
		StreamOffset{Stream: "gotestStream2", Offset: StreamNeverDeliveredOffset},
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

	var msgCnt int = 0

	run := true
	for run {
		select {
		case <-ctx.Done():
			t.Logf("done")
			c.close()
			run = false

		default:
			res, err := c.read(8, 10*time.Millisecond)
			// t.Logf("%+v", res)
			if err != nil {
				if err != redis.Nil {
					t.Errorf("%+v\n", err)
					return
				}
			} else {
				if len(res) > 0 {
					for _, stream := range res {
						for _, message := range stream.Messages {
							log.Printf("Stream: %s, Message:%+v\n", stream.Stream, message)
							c.client.XAck(stream.Stream, c.Group, message.ID)
							msgCnt++
						}
					}
				}
			}
		}
	}

	// assert
	{
		var expectedMsgCnt int = 4
		if msgCnt != expectedMsgCnt {
			t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
		}
	}
}

func TestConsumerClient_Claim(t *testing.T) {
	{
		/*
			XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM
			XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM

			XADD gotestStream1 * name luffy age 19
			XADD gotestStream1 * name nami age 21
			XADD gotestStream2 * name roger age ??
			XADD gotestStream2 * name ace age 22

			XREADGROUP GROUP gotestGroup gotest-main COUNT 8 STREAMS gotestStream1 gotestStream2 > >

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
			"XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM",
			"XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM",

			"XADD gotestStream1 * name luffy age 19",
			"XADD gotestStream1 * name nami age 21",
			"XADD gotestStream2 * name roger age ??",
			"XADD gotestStream2 * name ace age 22",

			"XREADGROUP GROUP gotestGroup gotest-main COUNT 8 STREAMS gotestStream1 gotestStream2 > >",
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

	c := &ConsumerClient{
		Group:       "gotestGroup",
		Name:        "gotestConsumer",
		RedisOption: &opt,
	}

	err := c.subscribe(
		StreamOffset{Stream: "gotestStream1", Offset: StreamNeverDeliveredOffset},
		StreamOffset{Stream: "gotestStream2", Offset: StreamNeverDeliveredOffset},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()

	var msgCnt int = 0
	time.Sleep(4 * time.Second)

	res, err := c.claim(4*time.Second, 1, 10)
	// t.Logf("%+v", res)
	if err != nil {
		if err != redis.Nil {
			t.Errorf("%+v\n", err)
			return
		}
	} else {
		if len(res) > 0 {
			for _, stream := range res {
				for _, message := range stream.Messages {
					log.Printf("Stream: %s, Message:%+v\n", stream.Stream, message)
					c.client.XAck(stream.Stream, c.Group, message.ID)
					msgCnt++
				}
			}
		}
	}

	// assert
	{
		var expectedMsgCnt int = 2
		if msgCnt != expectedMsgCnt {
			t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
		}
	}
}

func TestConsumerClient_Read_WithPause(t *testing.T) {
	{
		/*
			XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM
			XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM

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
			"XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM",
			"XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM",

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

	c := &ConsumerClient{
		Group:       "gotestGroup",
		Name:        "gotestConsumer",
		RedisOption: &opt,
	}

	err := c.subscribe(
		StreamOffset{Stream: "gotestStream1", Offset: StreamNeverDeliveredOffset},
		StreamOffset{Stream: "gotestStream2", Offset: StreamNeverDeliveredOffset},
	)
	if err != nil {
		t.Fatal(err)
	}

	err = c.pause("gotestStream1")
	if err != nil {
		t.Fatal(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

	var msgCnt int = 0

	run := true
	for run {
		select {
		case <-ctx.Done():
			t.Logf("done")
			c.close()
			run = false

		default:
			res, err := c.read(8, 10*time.Millisecond)
			// t.Logf("%+v", res)
			if err != nil {
				if err != redis.Nil {
					t.Errorf("%+v\n", err)
					return
				}
			} else {
				if len(res) > 0 {
					for _, stream := range res {
						for _, message := range stream.Messages {
							log.Printf("Stream: %s, Message:%+v\n", stream.Stream, message)
							c.client.XAck(stream.Stream, c.Group, message.ID)
							msgCnt++
						}
					}
				}
			}
		}
	}

	// assert
	{
		var expectedMsgCnt int = 2
		if msgCnt != expectedMsgCnt {
			t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
		}
	}
}

func TestConsumerClient_Claim_WithPause(t *testing.T) {
	{
		/*
			XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM
			XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM

			XADD gotestStream1 * name luffy age 19
			XADD gotestStream1 * name nami age 21
			XADD gotestStream2 * name roger age ??
			XADD gotestStream2 * name ace age 22

			XREADGROUP GROUP gotestGroup gotest-main COUNT 8 STREAMS gotestStream1 gotestStream2 > >

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
			"XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM",
			"XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM",

			"XADD gotestStream1 * name luffy age 19",
			"XADD gotestStream1 * name nami age 21",
			"XADD gotestStream2 * name roger age ??",
			"XADD gotestStream2 * name ace age 22",

			"XREADGROUP GROUP gotestGroup gotest-main COUNT 8 STREAMS gotestStream1 gotestStream2 > >",
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

	c := &ConsumerClient{
		Group:       "gotestGroup",
		Name:        "gotestConsumer",
		RedisOption: &opt,
	}

	err := c.subscribe(
		StreamOffset{Stream: "gotestStream1", Offset: StreamNeverDeliveredOffset},
		StreamOffset{Stream: "gotestStream2", Offset: StreamNeverDeliveredOffset},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()

	err = c.pause("gotestStream1")
	if err != nil {
		t.Fatal(err)
	}

	var msgCnt int = 0
	time.Sleep(4 * time.Second)

	res, err := c.claim(4*time.Second, 1, 10)
	// t.Logf("%+v", res)
	if err != nil {
		if err != redis.Nil {
			t.Errorf("%+v\n", err)
			return
		}
	} else {
		if len(res) > 0 {
			for _, stream := range res {
				for _, message := range stream.Messages {
					log.Printf("Stream: %s, Message:%+v\n", stream.Stream, message)
					c.client.XAck(stream.Stream, c.Group, message.ID)
					msgCnt++
				}
			}
		}
	}

	// assert
	{
		var expectedMsgCnt int = 1
		if msgCnt != expectedMsgCnt {
			t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
		}
	}
}

func TestConsumerClient_Read_WithPauseAll(t *testing.T) {
	{
		/*
			XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM
			XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM

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
			"XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM",
			"XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM",

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

	c := &ConsumerClient{
		Group:       "gotestGroup",
		Name:        "gotestConsumer",
		RedisOption: &opt,
	}

	err := c.subscribe(
		StreamOffset{Stream: "gotestStream1", Offset: StreamNeverDeliveredOffset},
		StreamOffset{Stream: "gotestStream2", Offset: StreamNeverDeliveredOffset},
	)
	if err != nil {
		t.Fatal(err)
	}

	err = c.pause("gotestStream1", "gotestStream2")
	if err != nil {
		t.Fatal(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

	var msgCnt int = 0

	run := true
	for run {
		select {
		case <-ctx.Done():
			t.Logf("done")
			c.close()
			run = false

		default:
			res, err := c.read(8, 10*time.Millisecond)
			// t.Logf("%+v", res)
			if err != nil {
				if err != redis.Nil {
					t.Errorf("%+v\n", err)
					return
				}
			} else {
				if len(res) > 0 {
					for _, stream := range res {
						for _, message := range stream.Messages {
							log.Printf("Stream: %s, Message:%+v\n", stream.Stream, message)
							c.client.XAck(stream.Stream, c.Group, message.ID)
							msgCnt++
						}
					}
				}
			}
		}
	}

	// assert
	{
		var expectedMsgCnt int = 0
		if msgCnt != expectedMsgCnt {
			t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
		}
	}
}

func TestConsumerClient_Claim_WithPauseAll(t *testing.T) {
	{
		/*
			XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM
			XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM

			XADD gotestStream1 * name luffy age 19
			XADD gotestStream1 * name nami age 21
			XADD gotestStream2 * name roger age ??
			XADD gotestStream2 * name ace age 22

			XREADGROUP GROUP gotestGroup gotest-main COUNT 8 STREAMS gotestStream1 gotestStream2 > >

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
			"XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM",
			"XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM",

			"XADD gotestStream1 * name luffy age 19",
			"XADD gotestStream1 * name nami age 21",
			"XADD gotestStream2 * name roger age ??",
			"XADD gotestStream2 * name ace age 22",

			"XREADGROUP GROUP gotestGroup gotest-main COUNT 8 STREAMS gotestStream1 gotestStream2 > >",
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

	c := &ConsumerClient{
		Group:       "gotestGroup",
		Name:        "gotestConsumer",
		RedisOption: &opt,
	}

	err := c.subscribe(
		StreamOffset{Stream: "gotestStream1", Offset: StreamNeverDeliveredOffset},
		StreamOffset{Stream: "gotestStream2", Offset: StreamNeverDeliveredOffset},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()

	err = c.pause("gotestStream1", "gotestStream2")
	if err != nil {
		t.Fatal(err)
	}

	var msgCnt int = 0
	time.Sleep(4 * time.Second)

	res, err := c.claim(4*time.Second, 1, 10)
	// t.Logf("%+v", res)
	if err != nil {
		if err != redis.Nil {
			t.Errorf("%+v\n", err)
			return
		}
	} else {
		if len(res) > 0 {
			for _, stream := range res {
				for _, message := range stream.Messages {
					log.Printf("Stream: %s, Message:%+v\n", stream.Stream, message)
					c.client.XAck(stream.Stream, c.Group, message.ID)
					msgCnt++
				}
			}
		}
	}

	// assert
	{
		var expectedMsgCnt int = 0
		if msgCnt != expectedMsgCnt {
			t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
		}
	}
}

func TestConsumerClient_Read_WithPauseAndResume(t *testing.T) {
	{
		/*
			XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM
			XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM

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
			"XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM",
			"XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM",

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

	c := &ConsumerClient{
		Group:       "gotestGroup",
		Name:        "gotestConsumer",
		RedisOption: &opt,
	}

	err := c.subscribe(
		StreamOffset{Stream: "gotestStream1", Offset: StreamNeverDeliveredOffset},
		StreamOffset{Stream: "gotestStream2", Offset: StreamNeverDeliveredOffset},
	)
	if err != nil {
		t.Fatal(err)
	}

	err = c.pause("gotestStream1")
	if err != nil {
		t.Fatal(err)
	}
	err = c.resume("gotestStream1")
	if err != nil {
		t.Fatal(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

	var msgCnt int = 0

	run := true
	for run {
		select {
		case <-ctx.Done():
			t.Logf("done")
			c.close()
			run = false

		default:
			res, err := c.read(8, 10*time.Millisecond)
			// t.Logf("%+v", res)
			if err != nil {
				if err != redis.Nil {
					t.Errorf("%+v\n", err)
					return
				}
			} else {
				if len(res) > 0 {
					for _, stream := range res {
						for _, message := range stream.Messages {
							log.Printf("Stream: %s, Message:%+v\n", stream.Stream, message)
							c.client.XAck(stream.Stream, c.Group, message.ID)
							msgCnt++
						}
					}
				}
			}
		}
	}

	// assert
	{
		var expectedMsgCnt int = 4
		if msgCnt != expectedMsgCnt {
			t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
		}
	}
}

func TestConsumerClient_Claim_WithPauseAndResume(t *testing.T) {
	{
		/*
			XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM
			XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM

			XADD gotestStream1 * name luffy age 19
			XADD gotestStream1 * name nami age 21
			XADD gotestStream2 * name roger age ??
			XADD gotestStream2 * name ace age 22

			XREADGROUP GROUP gotestGroup gotest-main COUNT 8 STREAMS gotestStream1 gotestStream2 > >

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
			"XGROUP CREATE gotestStream1 gotestGroup $ MKSTREAM",
			"XGROUP CREATE gotestStream2 gotestGroup $ MKSTREAM",

			"XADD gotestStream1 * name luffy age 19",
			"XADD gotestStream1 * name nami age 21",
			"XADD gotestStream2 * name roger age ??",
			"XADD gotestStream2 * name ace age 22",

			"XREADGROUP GROUP gotestGroup gotest-main COUNT 8 STREAMS gotestStream1 gotestStream2 > >",
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

	c := &ConsumerClient{
		Group:       "gotestGroup",
		Name:        "gotestConsumer",
		RedisOption: &opt,
	}

	err := c.subscribe(
		StreamOffset{Stream: "gotestStream1", Offset: StreamNeverDeliveredOffset},
		StreamOffset{Stream: "gotestStream2", Offset: StreamNeverDeliveredOffset},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()

	err = c.pause("gotestStream1")
	if err != nil {
		t.Fatal(err)
	}
	err = c.resume("gotestStream1")
	if err != nil {
		t.Fatal(err)
	}

	var msgCnt int = 0
	time.Sleep(4 * time.Second)

	res, err := c.claim(4*time.Second, 1, 10)
	// t.Logf("%+v", res)
	if err != nil {
		if err != redis.Nil {
			t.Errorf("%+v\n", err)
			return
		}
	} else {
		if len(res) > 0 {
			for _, stream := range res {
				for _, message := range stream.Messages {
					log.Printf("Stream: %s, Message:%+v\n", stream.Stream, message)
					c.client.XAck(stream.Stream, c.Group, message.ID)
					msgCnt++
				}
			}
		}
	}

	// assert
	{
		var expectedMsgCnt int = 2
		if msgCnt != expectedMsgCnt {
			t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
		}
	}
}
