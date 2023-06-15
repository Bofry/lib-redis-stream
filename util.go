package redis

import (
	"fmt"

	redis "github.com/go-redis/redis/v7"
)

func createRedisUniversalClient(opt *UniversalOptions) (UniversalClient, error) {
	client := redis.NewUniversalClient(opt)
	if client == nil {
		return nil, fmt.Errorf("fail to create redis.Client")
	}

	_, err := client.Ping().Result()
	if err != nil {
		if err != redis.Nil {
			return nil, err
		}
	}
	return client, nil
}

func assertCompatibility(condition bool, message string) {
	if !condition {
		panic(fmt.Sprintf("unsupported Redis version. %s", message))
	}
}
