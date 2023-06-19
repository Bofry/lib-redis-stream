package redis

import (
	_ "unsafe"
)

var _ error = new(ConsumerError)

type ConsumerError struct {
	err error
}

func (e *ConsumerError) Error() string {
	return e.Error()
}

func (e *ConsumerError) Unwrap() error {
	return e.err
}

func (e *ConsumerError) IsRedisError() bool {
	_, ok := e.err.(RedisError)
	return ok
}
