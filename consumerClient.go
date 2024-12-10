package redis

import (
	"fmt"
	"sync"
	"time"

	redis "github.com/go-redis/redis/v7"
)

type consumerClient struct {
	Group       string
	Name        string
	RedisOption *redis.UniversalOptions

	client UniversalClient
	wg     sync.WaitGroup

	streams []StreamOffsetInfo
	// streamKeyState   map[string]bool
	streamKeyState   *sync.Map
	streamKeys       []string
	streamKeyOffsets []string

	mutex    sync.Mutex
	running  bool
	disposed bool
}

func (c *consumerClient) subscribe(streams ...StreamOffsetInfo) error {
	if len(streams) == 0 {
		return fmt.Errorf("specified streams is empty")
	}
	if c.disposed {
		return fmt.Errorf("the Consumer has been disposed")
	}
	if c.running {
		return fmt.Errorf("the Consumer is running")
	}

	var (
		size = len(streams)
		// keyState = make(map[string]bool)
		keyState = new(sync.Map)
		keys     = make([]string, 0, size)
	)

	var err error
	c.mutex.Lock()
	defer func() {
		if err != nil {
			c.running = false
			c.disposed = true
		}
		c.mutex.Unlock()
	}()
	c.running = true

	// init clent
	if err = c.configRedisClient(); err != nil {
		return err
	}

	if size > 0 {
		for i := 0; i < size; i++ {
			s := streams[i]
			k := s.getStreamOffset().Stream
			keyState.Store(k, true)
			keys = append(keys, k)
		}
	}
	c.streams = streams
	c.streamKeys = keys
	c.streamKeyState = keyState
	c.updateStreamKeyOffset()

	return nil
}

func (c *consumerClient) claim(minIdleTime time.Duration, count int64, pendingFetchingSize int64) ([]redis.XStream, error) {
	if c.disposed {
		return nil, fmt.Errorf("the Consumer has been disposed")
	}
	if !c.running {
		return nil, fmt.Errorf("the Consumer is not running")
	}

	c.wg.Add(1)
	defer c.wg.Done()

	var resultStream []redis.XStream = make([]redis.XStream, 0, len(c.streamKeys))
	for _, stream := range c.streamKeys {
		if !c.isConnected(stream) {
			continue
		}

		// fetch all pending messages from specified redis stream key
		pendingSet, err := c.client.XPendingExt(&redis.XPendingExtArgs{
			Stream: stream,
			Group:  c.Group,
			Start:  "-",
			End:    "+",
			Count:  pendingFetchingSize,
		}).Result()
		if err != nil {
			if err != redis.Nil {
				return nil, err
			}
		}

		if len(pendingSet) > 0 {
			var (
				pendingMessageIDs []string = make([]string, 0, count)
			)

			// filter the message ids that only the idle time over
			// the Worker.ClaimMinIdleTime
			for _, pending := range pendingSet {
				// update the last pending id
				if pending.Idle >= minIdleTime {
					pendingMessageIDs = append(pendingMessageIDs, pending.ID)

					if len(pendingMessageIDs) == int(count) {
						break
					}
				}
			}

			if len(pendingMessageIDs) > 0 {
				claimMessages, err := c.client.XClaim(&redis.XClaimArgs{
					Stream:   stream,
					Group:    c.Group,
					Consumer: c.Name,
					MinIdle:  minIdleTime,
					Messages: pendingMessageIDs,
				}).Result()
				if err != nil {
					if err != redis.Nil {
						return nil, err
					}
				}

				// clear invalid message IDs (ghost IDs)
				if len(claimMessages) != len(pendingMessageIDs) {
					assertCompatibility(
						len(claimMessages) < len(pendingMessageIDs),
						"the XCLAIM messages must less or equal than the XPENDING messages")

					if len(claimMessages) == 0 {
						// purge ghost IDs
						if err := c.ackGhostIDs(stream, pendingMessageIDs...); err != nil {
							return nil, err
						}
					} else {
						var (
							ghostIDs               []string
							nextClaimMessagesIndex int = 0
						)

						// NOTE: Because
						//   1) the XCLAIM messages must less or equal than the XPENDING messages,
						//   2) the XCLAIM messages might be missing part messages but it won't change sequence,
						// we can check XCLAIM messages according to XPENDING messages sequence with their message ID.
						for i, id := range pendingMessageIDs {
							if nextClaimMessagesIndex < len(claimMessages) {
								if id == claimMessages[nextClaimMessagesIndex].ID {
									// advence nextMessagesIndex
									nextClaimMessagesIndex++
								} else {
									ghostIDs = append(ghostIDs, id)
								}
							} else {
								ghostIDs = append(ghostIDs, pendingMessageIDs[i:]...)
								break
							}
						}

						// purge ghost IDs
						if err := c.ackGhostIDs(stream, ghostIDs...); err != nil {
							return nil, err
						}
					}
				}

				if len(claimMessages) > 0 {
					resultStream = append(resultStream, redis.XStream{
						Stream:   stream,
						Messages: claimMessages,
					})
				}
			}
		}
	}
	return resultStream, nil
}

func (c *consumerClient) read(count int64, timeout time.Duration) ([]redis.XStream, error) {
	if c.disposed {
		return nil, fmt.Errorf("the Consumer has been disposed")
	}
	if !c.running {
		return nil, fmt.Errorf("the Consumer is not running")
	}

	// return nil if unset stream offset
	if len(c.streamKeyOffsets) == 0 {
		return nil, nil
	}

	c.wg.Add(1)
	defer c.wg.Done()

	messages, err := c.client.XReadGroup(&redis.XReadGroupArgs{
		Group:    c.Group,
		Consumer: c.Name,
		Count:    count,
		Streams:  c.streamKeyOffsets,
		Block:    timeout,
	}).Result()
	if err != nil {
		if err != redis.Nil {
			return nil, err
		}
	}
	return messages, nil
}

func (c *consumerClient) ack(key string, id ...string) (int64, error) {
	if c.disposed {
		return 0, fmt.Errorf("the Consumer has been disposed")
	}
	if !c.running {
		return 0, fmt.Errorf("the Consumer is not running")
	}

	c.wg.Add(1)
	defer c.wg.Done()

	reply, err := c.client.XAck(key, c.Group, id...).Result()
	if err != nil {
		if err != redis.Nil {
			return 0, err
		}
	}
	return reply, nil
}

func (c *consumerClient) del(key string, id ...string) (int64, error) {
	if c.disposed {
		return 0, fmt.Errorf("the Consumer has been disposed")
	}
	if !c.running {
		return 0, fmt.Errorf("the Consumer is not running")
	}

	c.wg.Add(1)
	defer c.wg.Done()

	reply, err := c.client.XDel(key, id...).Result()
	if err != nil {
		if err != redis.Nil {
			return 0, err
		}
	}
	return reply, nil
}

func (c *consumerClient) close() {
	if c.disposed {
		return
	}

	c.mutex.Lock()
	defer func() {
		c.running = false
		c.disposed = true

		c.mutex.Unlock()
	}()

	c.wg.Wait()
	c.client.Close()
}

func (c *consumerClient) configRedisClient() error {
	if c.client == nil {
		client, err := createRedisUniversalClient(c.RedisOption)
		if err != nil {
			return err
		}

		c.client = client
	}
	return nil
}

func (c *consumerClient) ackGhostIDs(stream string, ghostIDs ...string) error {
	for _, id := range ghostIDs {
		reply, err := c.client.XRange(stream, id, id).Result()
		if err != nil {
			if err != redis.Nil {
				return err
			}
		}

		if len(reply) == 0 {
			err = c.client.XAck(stream, c.Group, id).Err()
			if err != nil {
				if err != redis.Nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *consumerClient) pause(streams ...string) error {
	for _, s := range streams {
		// if _, ok := c.streamKeyState[s]; ok {
		// 	c.streamKeyState[s] = false
		// }
		if _, ok := c.streamKeyState.Load(s); ok {
			c.streamKeyState.Store(s, false)
		}
	}
	c.updateStreamKeyOffset()
	return nil
}

func (c *consumerClient) resume(streams ...string) error {
	for _, s := range streams {
		// if _, ok := c.streamKeyState[s]; ok {
		// 	c.streamKeyState[s] = true
		// }
		if _, ok := c.streamKeyState.Load(s); ok {
			c.streamKeyState.Store(s, true)
		}
	}
	c.updateStreamKeyOffset()
	return nil
}

func (c *consumerClient) isConnected(stream string) bool {
	// if v, ok := c.streamKeyState[stream]; ok {
	// 	return v
	// }
	if v, ok := c.streamKeyState.Load(stream); ok {
		return v.(bool)
	}
	c.updateStreamKeyOffset()
	return false
}

func (c *consumerClient) updateStreamKeyOffset() {
	var (
		size       = len(c.streams)
		streams    = c.streams
		keys       = make([]string, 0, size)
		keyOffsets = make([]string, 0, size*2)
	)
	if size > 0 {
		for i := 0; i < size; i++ {
			s := streams[i]
			k := s.getStreamOffset().Stream
			if c.isConnected(k) {
				keys = append(keys, k)
			}
		}
		keyOffsets = append(keyOffsets, keys...)
		for i := 0; i < size; i++ {
			s := streams[i]
			k := s.getStreamOffset().Stream
			if c.isConnected(k) {
				offset := StreamNeverDeliveredOffset
				if len(s.getStreamOffset().Offset) > 0 {
					offset = s.getStreamOffset().Offset
				}
				keyOffsets = append(keyOffsets, offset)
			}
		}
	}
	c.streamKeyOffsets = keyOffsets
}
