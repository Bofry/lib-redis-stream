package redis

import (
	"fmt"
	"log"
	"sync"
	"time"

	redis "github.com/go-redis/redis/v7"
)

type Consumer struct {
	Group               string
	Name                string
	RedisOption         *redis.UniversalOptions
	MaxInFlight         int64
	MaxPollingTimeout   time.Duration
	ClaimMinIdleTime    time.Duration
	IdlingTimeout       time.Duration // 若沒有任何訊息時等待多久
	ClaimSensitivity    int           // Read 時取得的訊息數小於 n 的話, 執行 Claim
	ClaimOccurrenceRate int32         // Read 每執行 n 次後 執行 Claim 1 次
	MessageHandler      MessageHandleProc
	RedisErrorHandler   RedisErrorHandleProc
	Logger              *log.Logger

	client   *ConsumerClient
	stopChan chan bool
	wg       sync.WaitGroup

	claimTrigger *CyclicCounter

	mutex       sync.Mutex
	initialized bool
	running     bool
	disposed    bool
}

func (c *Consumer) Subscribe(streams ...StreamOffsetInfo) error {
	if c.disposed {
		return fmt.Errorf("the Consumer has been disposed")
	}
	if c.running {
		return fmt.Errorf("the Consumer is running")
	}

	var err error
	c.mutex.Lock()
	defer func() {
		if err != nil {
			c.running = false
			c.disposed = true
		}
		c.mutex.Unlock()
	}()

	if len(streams) == 0 {
		return nil
	}
	c.init()
	c.running = true

	// new consumer
	{
		consumer := &ConsumerClient{
			Group:       c.Group,
			Name:        c.Name,
			RedisOption: c.RedisOption,
		}

		err = consumer.subscribe(streams...)
		if err != nil {
			return err
		}

		c.client = consumer
	}

	// reset
	c.claimTrigger.reset()

	go func() {
		c.wg.Add(1)
		defer c.wg.Done()

		defer c.client.close()

		for {
			select {
			case <-c.stopChan:
				return

			default:
				err := c.processMessage()
				if err != nil {
					if !c.processRedisError(err) {
						c.Logger.Fatalf("%% Error: %v\n", err)
						return
					}
				}
			}
		}
	}()
	return nil
}

func (c *Consumer) Close() {
	if c.disposed {
		return
	}

	c.mutex.Lock()
	defer func() {
		c.running = false
		c.disposed = true

		c.mutex.Unlock()
	}()

	if c.stopChan != nil {
		c.stopChan <- true
		close(c.stopChan)
	}

	c.wg.Wait()
}

func (c *Consumer) init() {
	if c.initialized {
		return
	}

	if c.stopChan == nil {
		c.stopChan = make(chan bool, 1)
	}

	if c.claimTrigger == nil {
		c.claimTrigger = newCyclicCounter(c.ClaimOccurrenceRate)
	}

	if c.Logger == nil {
		c.Logger = defaultLogger
	}
	c.initialized = true
}

func (c *Consumer) processRedisError(err error) (disposed bool) {
	if c.RedisErrorHandler != nil {
		consumerErr := &ConsumerError{
			err: err,
		}
		return c.RedisErrorHandler(consumerErr)
	}
	return false
}

func (c *Consumer) getRedisClient() redis.UniversalClient {
	return c.client.client
}

func (c *Consumer) processMessage() error {
	var (
		readMessages int = 0
	)

	// perform XREADGROUP
	{
		streams, err := c.client.read(c.MaxInFlight, c.MaxPollingTimeout)
		if err != nil {
			if err != redis.Nil {
				return err
			}
		}

		if len(streams) > 0 {
			for _, stream := range streams {
				for _, message := range stream.Messages {
					c.handleMessage(stream.Stream, &message)
					readMessages++
				}
			}
		}
	}

	// perform XAUTOCLAIM
	if c.claimTrigger.spin() || readMessages < c.ClaimSensitivity {
		// fmt.Println("***CLAIM")
		var (
			pendingFetchingSize = c.computePendingFetchingSize(c.MaxInFlight)
		)

		streams, err := c.client.claim(c.ClaimMinIdleTime, c.MaxInFlight, pendingFetchingSize)
		if err != nil {
			if err != redis.Nil {
				return err
			}
		}
		if len(streams) > 0 {
			for _, stream := range streams {
				for _, message := range stream.Messages {
					c.handleMessage(stream.Stream, &message)
				}
			}
			return nil
		}

		if readMessages == 0 {
			time.Sleep(c.IdlingTimeout)
		}
	}
	return nil
}

func (c *Consumer) computePendingFetchingSize(maxInFlight int64) int64 {
	var (
		fetchingSize = maxInFlight * PENDING_FETCHING_SIZE_COEFFICIENT
	)

	if fetchingSize < MIN_PENDING_FETCHING_SIZE {
		return MIN_PENDING_FETCHING_SIZE
	}
	if fetchingSize > MAX_PENDING_FETCHING_SIZE {
		return MAX_PENDING_FETCHING_SIZE
	}
	return fetchingSize
}

func (c *Consumer) handleMessage(stream string, m *redis.XMessage) {
	msg := Message{
		XMessage:      m,
		ConsumerGroup: c.Group,
		Stream:        stream,
		Delegate:      &clientMessageDelegate{client: c},
	}

	c.MessageHandler(&msg)
}

func (c *Consumer) doAck(m *Message) {
	if c.disposed {
		return
	}
	if !c.running {
		return
	}

	_, err := c.client.ack(m.Stream, m.ID)
	if err != nil {
		c.Logger.Printf("error sending command XACK '%s' '%s' '%s'", m.Stream, c.Group, m.ID)
	}
}

func (c *Consumer) doDel(m *Message) {
	if c.disposed {
		return
	}
	if !c.running {
		return
	}

	_, err := c.client.del(m.Stream, m.ID)
	if err != nil {
		c.Logger.Printf("error sending command XACK '%s' '%s'", m.Stream, m.ID)
	}
}
