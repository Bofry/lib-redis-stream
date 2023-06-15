package tracing

import "go.opentelemetry.io/otel/propagation"

var _ propagation.TextMapCarrier = new(MessageStateCarrier)

type MessageStateCarrier struct {
	value MessageState
}

func NewMessageStateCarrier(state MessageState) *MessageStateCarrier {
	return &MessageStateCarrier{
		value: state,
	}
}

// Get returns the value associated with the passed key.
func (hc MessageStateCarrier) Get(key string) string {
	reply := hc.value.Value(key)
	if reply != nil {
		if v, ok := reply.(string); ok {
			return v
		}
	}
	return ""
}

// Set stores the key-value pair.
func (hc MessageStateCarrier) Set(key string, value string) {
	hc.value.Set(key, value)
}

// Keys lists the keys stored in this carrier.
func (hc MessageStateCarrier) Keys() []string {
	state := hc.value
	keys := make([]string, 0, state.Len())
	state.Visit(func(key string, value interface{}) {
		keys = append(keys, key)
	})
	return keys
}
