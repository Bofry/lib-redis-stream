package redis

import "log"

type ProducerConfig struct {
	*UniversalOptions

	Logger *log.Logger
}
