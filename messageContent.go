package redis

import "strings"

const (
	_DefaultMessageStateKeyPrefix = "header:"
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

	if c.State.contentKeyPrefix == "" {
		c.State.contentKeyPrefix = _DefaultMessageStateKeyPrefix
	}

	c.State.Visit(func(name string, value interface{}) {
		var key = c.State.contentKeyPrefix + name
		container[key] = value
	})

	for k, v := range c.Values {
		container[k] = v
	}
}

func DecodeMessageContent(container map[string]interface{}, opts ...DecodeMessageContentOption) *MessageContent {
	var (
		content MessageContent = MessageContent{}
		values  map[string]interface{}
	)

	if container == nil {
		return nil
	}

	values = make(map[string]interface{})

	var setting = &DecodeMessageContentSetting{
		MessageStateKeyPrefix: _DefaultMessageStateKeyPrefix,
	}

	for _, opt := range opts {
		opt.apply(setting)
	}

	content.State.contentKeyPrefix = setting.MessageStateKeyPrefix

	for k, v := range container {
		if len(setting.MessageStateKeyPrefix) > 0 {
			key, ok := strings.CutPrefix(k, content.State.contentKeyPrefix)
			if ok {
				content.State.Set(key, v)
				continue
			}
		}
		values[k] = v
	}

	content.Values = values
	return &content
}
