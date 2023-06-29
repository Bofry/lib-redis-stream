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

func TestMessage_Clone(t *testing.T) {
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

	cloned := m.Clone()

	var expectedConsumerGroup string = m.ConsumerGroup
	if expectedConsumerGroup != cloned.ConsumerGroup {
		t.Errorf("cloned.ConsumerGroup expect:: %v, got:: %v\n", expectedConsumerGroup, cloned.ConsumerGroup)
	}
	var expectedStream string = m.Stream
	if expectedStream != cloned.Stream {
		t.Errorf("cloned.Stream expect:: %v, got:: %v\n", expectedStream, cloned.Stream)
	}
	var expectedDelegate MessageDelegate = m.Delegate
	if expectedDelegate != cloned.Delegate {
		t.Errorf("cloned.Delegate expect:: %v, got:: %v\n", expectedDelegate, cloned.Delegate)
	}
	var expectedXMessage *XMessage = m.XMessage
	if expectedXMessage != cloned.XMessage {
		t.Errorf("cloned.XMessage expect:: %v, got:: %v\n", expectedXMessage, cloned.XMessage)
	}
}
