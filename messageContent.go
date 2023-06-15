package redis

import "strings"

const (
	_MessageStateKeyPrefix = "header:"
)

type MessageContent struct {
	State  MessageState
	Values map[string]interface{}
}

func NewMessageContent() *MessageContent {
	return &MessageContent{
		State:  MessageState{},
		Values: make(map[string]interface{}),
	}
}

func (c *MessageContent) WriteTo(container map[string]interface{}) {
	if container == nil {
		panic("call MessageContent.WriteTo() use a nil container")
	}

	c.State.Visit(func(name string, value interface{}) {
		var key = _MessageStateKeyPrefix + name
		container[key] = value
	})

	for k, v := range c.Values {
		container[k] = v
	}
}

func DecodeMessageContent(container map[string]interface{}) *MessageContent {
	var (
		content MessageContent = MessageContent{}
		values  map[string]interface{}
	)

	if container == nil {
		return nil
	}

	values = make(map[string]interface{})

	for k, v := range container {
		key, ok := strings.CutPrefix(k, _MessageStateKeyPrefix)
		if ok {
			content.State.Set(key, v)
		} else {
			values[key] = v
		}
	}

	content.Values = values
	return &content
}
