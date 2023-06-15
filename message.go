package redis

type Message struct {
	*XMessage

	ConsumerGroup string
	Stream        string

	client *ConsumerClient
}

func (m *Message) Ack() error {
	_, err := m.client.ack(m.Stream, m.ID)
	return err
}

func (m *Message) Del() error {
	_, err := m.client.del(m.Stream, m.ID)
	return err
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
