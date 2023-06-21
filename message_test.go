package redis

import (
	"testing"

	redis "github.com/go-redis/redis/v7"
)

var _ MessageDelegate = new(mockMessageDelegate)

type mockMessageDelegate struct {
	ackCalledCount int
	delCalledCount int
}

// OnAck implements MessageDelegate.
func (d *mockMessageDelegate) OnAck(msg *Message) {
	d.ackCalledCount++
}

// OnDel implements MessageDelegate.
func (d *mockMessageDelegate) OnDel(msg *Message) {
	d.delCalledCount++
}

func TestMessage(t *testing.T) {
	d := new(mockMessageDelegate)

	m := Message{
		ConsumerGroup: "go-test-channel",
		Stream:        "goTestStream",
		Delegate:      d,
		XMessage: &redis.XMessage{
			ID: "1000",
			Values: map[string]interface{}{
				"foo": "bar",
			},
		},
	}

	{
		var expectedAckCalledCount int = 0
		if expectedAckCalledCount != d.ackCalledCount {
			t.Errorf("mockMessageDelegate.ackCalledCount expect:: %v, got:: %v\n", expectedAckCalledCount, d.ackCalledCount)
		}
		var expectedDelCalledCount int = 0
		if expectedDelCalledCount != d.delCalledCount {
			t.Errorf("mockMessageDelegate.delCalledCount expect:: %v, got:: %v\n", expectedDelCalledCount, d.delCalledCount)
		}
	}

	m.Ack()
	{
		var expectedAckCalledCount int = 1
		if expectedAckCalledCount != d.ackCalledCount {
			t.Errorf("mockMessageDelegate.ackCalledCount expect:: %v, got:: %v\n", expectedAckCalledCount, d.ackCalledCount)
		}
		var expectedDelCalledCount int = 0
		if expectedDelCalledCount != d.delCalledCount {
			t.Errorf("mockMessageDelegate.delCalledCount expect:: %v, got:: %v\n", expectedDelCalledCount, d.delCalledCount)
		}
	}

	m.Del()
	{
		var expectedAckCalledCount int = 1
		if expectedAckCalledCount != d.ackCalledCount {
			t.Errorf("mockMessageDelegate.ackCalledCount expect:: %v, got:: %v\n", expectedAckCalledCount, d.ackCalledCount)
		}
		var expectedDelCalledCount int = 1
		if expectedDelCalledCount != d.delCalledCount {
			t.Errorf("mockMessageDelegate.delCalledCount expect:: %v, got:: %v\n", expectedDelCalledCount, d.delCalledCount)
		}
	}

	m.Ack()
	m.Del()
	{
		var expectedAckCalledCount int = 1
		if expectedAckCalledCount != d.ackCalledCount {
			t.Errorf("mockMessageDelegate.ackCalledCount expect:: %v, got:: %v\n", expectedAckCalledCount, d.ackCalledCount)
		}
		var expectedDelCalledCount int = 1
		if expectedDelCalledCount != d.delCalledCount {
			t.Errorf("mockMessageDelegate.delCalledCount expect:: %v, got:: %v\n", expectedDelCalledCount, d.delCalledCount)
		}
	}
}
