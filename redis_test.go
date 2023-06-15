package redis_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	redis "github.com/Bofry/lib-redis-stream"
	"github.com/joho/godotenv"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var (
	traceIDStr = "4bf92f3577b34da6a3ce929d0e0e4736"
	spanIDStr  = "00f067aa0ba902b7"

	__TEST_REDIS_SERVERS []string

	__ENV_FILE        = "redis_test.env"
	__ENV_FILE_SAMPLE = "redis_test.env.sample"

	__TEST_TRACE_ID = mustTraceIDFromHex(traceIDStr)
	__TEST_SPAN_ID  = mustSpanIDFromHex(spanIDStr)

	__TEST_PROPAGATOR = propagation.TraceContext{}
	__TEST_CONTEXT    = mustSpanContext()
)

func mustTraceIDFromHex(s string) (t trace.TraceID) {
	var err error
	t, err = trace.TraceIDFromHex(s)
	if err != nil {
		panic(err)
	}
	return
}

func mustSpanIDFromHex(s string) (t trace.SpanID) {
	var err error
	t, err = trace.SpanIDFromHex(s)
	if err != nil {
		panic(err)
	}
	return
}

func mustSpanContext() context.Context {
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    __TEST_TRACE_ID,
		SpanID:     __TEST_SPAN_ID,
		TraceFlags: 0,
	})
	return trace.ContextWithSpanContext(context.Background(), sc)
}

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

func TestMain(m *testing.M) {
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
		__TEST_REDIS_SERVERS = strings.Split(env["TEST_REDIS_SERVERS"], ",")
	}
	m.Run()
}

func TestProducer_Write(t *testing.T) {
	// register consumer group
	{
		admin, err := redis.NewAdminClient(&redis.UniversalOptions{
			Addrs: __TEST_REDIS_SERVERS,
			DB:    0,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer admin.Close()

		/* NOTE: delete if existed !!
		DEL TestProducer_Write_1
		DEL TestProducer_Write_2
		*/
		_, err = admin.Handle().Del("TestProducer_Write_1", "TestProducer_Write_2").Result()
		if err != nil {
			t.Fatal(err)
		}

		/*
			XGROUP CREATE TestProducer_Write_1 gotestGroup $ MKSTREAM
			XGROUP CREATE TestProducer_Write_2 gotestGroup $ MKSTREAM
		*/
		for _, stream := range []string{"TestProducer_Write_1", "TestProducer_Write_2"} {
			_, err = admin.CreateConsumerGroupAndStream(stream, "gotestGroup", redis.StreamLastDeliveredID)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	defer func() {
		admin, err := redis.NewAdminClient(&redis.UniversalOptions{
			Addrs: __TEST_REDIS_SERVERS,
			DB:    0,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer admin.Close()

		/*
			XGROUP DESTROY TestProducer_Write_1 gotestGroup
			XGROUP DESTROY TestProducer_Write_2 gotestGroup
		*/
		for _, stream := range []string{"TestProducer_Write_1", "TestProducer_Write_2"} {
			_, err = admin.DeleteConsumerGroup(stream, "gotestGroup")
			if err != nil {
				t.Fatal(err)
			}
		}

		/*
			DEL TestProducer_Write_1
			DEL TestProducer_Write_2
		*/
		_, err = admin.Handle().Del("TestProducer_Write_1", "TestProducer_Write_2").Result()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// publish
	{
		conf := redis.ProducerConfig{
			UniversalOptions: &redis.UniversalOptions{
				Addrs: __TEST_REDIS_SERVERS,
				DB:    0,
			},
		}
		p, err := redis.NewProducer(&conf)
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()

		// produce message
		{
			messageTable := map[string][]map[string]interface{}{
				"TestProducer_Write_1": {
					{"name": "luffy", "age": 19},
					{"name": "nami", "age": 21},
					{"name": "zoro", "age": 21},
				},
				"TestProducer_Write_2": {
					{"name": "roger", "age": "??"},
					{"name": "ace", "age": "22"},
				},
			}

			for stream, messages := range messageTable {
				for _, message := range messages {
					reply, err := p.Write(stream, message)
					if err != nil {
						t.Fatal(err)
					}
					_ = reply
					t.Logf("ID: %s", reply)
				}
			}
		}

		// assert
		client := p.Handle()
		{
			msgCnt, err := client.XLen("TestProducer_Write_1").Result()
			if err != nil {
				t.Fatal(err)
			}
			var expectedMsgCnt int64 = 3
			if msgCnt != expectedMsgCnt {
				t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
			}
		}
		{
			msgCnt, err := client.XLen("TestProducer_Write_2").Result()
			if err != nil {
				t.Fatal(err)
			}
			var expectedMsgCnt int64 = 2
			if msgCnt != expectedMsgCnt {
				t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
			}
		}
	}

	// subscribe
	{
		// the config only for test use !!
		opt := redis.UniversalOptions{
			Addrs: __TEST_REDIS_SERVERS,
			DB:    0,
		}

		var msgCnt int = 0

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
				t.Logf("Message on %s: %v\n", message.Stream, message.XMessage)
				message.Ack()
				time.Sleep(500 * time.Millisecond)
				msgCnt++
			},
			ErrorHandler: func(err error) (disposed bool) {
				t.Fatal(err)
				return true
			},
		}

		err := c.Subscribe(
			redis.FromStreamNeverDeliveredOffset("TestProducer_Write_1"),
			redis.FromStreamNeverDeliveredOffset("TestProducer_Write_2"),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

		select {
		case <-ctx.Done():
			c.Close()
			break
		}

		// assert
		{
			var expectedMsgCnt int = 5
			if msgCnt != expectedMsgCnt {
				t.Errorf("expect %d messages, but got %d messages", expectedMsgCnt, msgCnt)
			}
		}
	}
}
