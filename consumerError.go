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
	return isRedisError(e.err)
}

//go:linkname isRedisError github.com/go-redis/redis/v7/internal/proto.isRedisError
func isRedisError(err error) bool
