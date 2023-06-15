package redis

import (
	"log"
	"os"

	redis "github.com/go-redis/redis/v7"
)

const (
	StreamAsteriskID           string = "*"
	StreamLastDeliveredID      string = "$"
	StreamZeroID               string = "0"
	StreamZeroOffset           string = "0"
	StreamNeverDeliveredOffset string = ">"

	Nil = redis.Nil

	LOGGER_PREFIX string = "[lib-redis-stream] "

	MAX_PENDING_FETCHING_SIZE         int64 = 4096
	MIN_PENDING_FETCHING_SIZE         int64 = 16
	PENDING_FETCHING_SIZE_COEFFICIENT int64 = 3
)

var (
	defaultLogger *log.Logger = log.New(os.Stdout, LOGGER_PREFIX, log.LstdFlags|log.Lmsgprefix)
)

type (
	UniversalOptions = redis.UniversalOptions
	UniversalClient  = redis.UniversalClient
	XMessage         = redis.XMessage
	XStream          = redis.XStream

	ProduceMessageOption interface {
		applyContent(msg *MessageContent) error
		applyID(id string) string
	}
)

// func
type (
	ErrorHandleProc   func(err error) (disposed bool)
	MessageHandleProc func(message *Message)
)

func DefaultLogger() *log.Logger {
	return defaultLogger
}
