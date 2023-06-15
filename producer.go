package redis

import (
	"fmt"
	"log"
	"sync"

	redis "github.com/go-redis/redis/v7"
)

type Producer struct {
	handle redis.UniversalClient

	logger *log.Logger

	wg          sync.WaitGroup
	mutex       sync.Mutex
	disposed    bool
	initialized bool
}

func NewProducer(config *ProducerConfig) (*Producer, error) {
	instance := &Producer{}

	var err error
	err = instance.init(config)
	if err != nil {
		return nil, err
	}
	return instance, nil
}

func (p *Producer) Handle() redis.UniversalClient {
	return p.handle
}

func (p *Producer) WriteContent(stream string, msg *MessageContent, opts ...ProduceMessageOption) (string, error) {
	if p.disposed {
		return "", fmt.Errorf("the Producer has been disposed")
	}
	if !p.initialized {
		p.logger.Panic("the Producer haven't be initialized yet")
	}

	id := StreamAsteriskID

	// apply options
	for _, opt := range opts {
		switch opt.(type) {
		case ProduceMessageContentOption:
			err := opt.applyContent(msg)
			if err != nil {
				return "", err
			}
		case ProduceMessageIDOption:
			id = opt.applyID(id)
		}
	}

	var values map[string]interface{}
	msg.WriteTo(values)
	return p.internalWrite(stream, id, values)
}

func (p *Producer) Write(stream string, values map[string]interface{}, opts ...ProduceMessageOption) (string, error) {
	if p.disposed {
		return "", fmt.Errorf("the Producer has been disposed")
	}
	if !p.initialized {
		p.logger.Panic("the Producer haven't be initialized yet")
	}

	id := StreamAsteriskID

	// apply options
	for _, opt := range opts {
		switch opt.(type) {
		case ProduceMessageIDOption:
			id = opt.applyID(id)
		}
	}

	fmt.Printf("ID: %s\n", id)

	return p.internalWrite(stream, id, values)
}

func (p *Producer) Close() {
	if p.disposed {
		return
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.disposed = true

	p.wg.Wait()
	p.handle.Close()
}

func (p *Producer) init(config *ProducerConfig) error {
	if p.initialized {
		return nil
	}

	client, err := createRedisUniversalClient(config.UniversalOptions)
	if err != nil {
		return err
	}

	// config logger
	p.configureLogger(config)

	p.handle = client

	p.initialized = true

	return nil
}

func (p *Producer) configureLogger(config *ProducerConfig) {
	if config.Logger != nil {
		p.logger = config.Logger
		return
	}
	p.logger = defaultLogger
}

func (p *Producer) internalWrite(stream string, id string, values map[string]interface{}) (string, error) {
	p.wg.Add(1)
	defer p.wg.Done()

	reply, err := p.handle.XAdd(&redis.XAddArgs{
		Stream: stream,
		ID:     id,
		Values: values,
	}).Result()
	if err != nil {
		if err != redis.Nil {
			return "", err
		}
	}
	return reply, nil
}
