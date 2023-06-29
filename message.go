package redis

import "sync/atomic"

type Message struct {
	*XMessage

	ConsumerGroup string
	Stream        string
	Delegate      MessageDelegate

	responded int32
	killed    int32
}

func (m *Message) Ack() {
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnAck(m)
}

func (m *Message) Del() {
	if !atomic.CompareAndSwapInt32(&m.killed, 0, 1) {
		return
	}
	m.Delegate.OnDel(m)
}

func (m *Message) HasResponded() bool {
	return atomic.LoadInt32(&m.responded) == 1 ||
		atomic.LoadInt32(&m.killed) == 1
}

func (m *Message) Content() *MessageContent {
	content := DecodeMessageContent(m.Values)
	if content != nil {
		return content
	}
	return &MessageContent{
		Values: m.Values,
	}
}

func (m *Message) Clone() *Message {
	return &Message{
		XMessage:      m.XMessage,
		ConsumerGroup: m.ConsumerGroup,
		Stream:        m.Stream,
		Delegate:      m.Delegate,
		responded:     m.responded,
		killed:        m.killed,
	}
}
