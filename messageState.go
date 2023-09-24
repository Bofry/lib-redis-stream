package redis

import (
	"fmt"
	"reflect"

	"github.com/Bofry/lib-redis-stream/tracing"
)

const (
	MESSAGE_STATE_NAME_MAX_LENGTH = 255
	MESSAGE_STATE_VALUE_MAX_SIZE  = 0x0fff
)

var _ tracing.MessageState = new(MessageState)

type MessageState struct {
	noCopy           noCopy
	contentKeyPrefix string
	values           map[string]interface{}
}

func (s *MessageState) Len() int {
	if s.values == nil {
		return 0
	}
	return len(s.values)
}

func (s *MessageState) Has(name string) bool {
	if s.values == nil {
		return false
	}

	_, ok := s.values[name]
	return ok
}

func (s *MessageState) Del(name string) interface{} {
	if s.values == nil {
		return nil
	}

	if v, ok := s.values[name]; ok {
		delete(s.values, name)
		return v
	}
	return nil
}

func (s *MessageState) SetString(name, value string) (old interface{}, err error) {
	return s.Set(name, value)
}

func (s *MessageState) Set(name string, value interface{}) (old interface{}, err error) {
	if err := s.validateName(name); err != nil {
		return nil, err
	}

	if s.values == nil {
		if value == nil {
			return nil, nil
		}

		if s.values == nil {
			s.values = make(map[string]interface{})
		}
	}
	// name existed?
	if v, ok := s.values[name]; ok {
		old = v

		// ignored when new value equals old value
		if reflect.DeepEqual(v, value) {
			return old, nil
		}

		// delete the key and exit when the value is nil
		if value == nil {
			delete(s.values, name)
			return old, nil
		}
	}

	if value != nil {
		s.values[name] = value
	}
	return old, nil
}

func (s *MessageState) Value(name string) interface{} {
	if s.values == nil {
		return nil
	}

	return s.values[name]
}

func (s *MessageState) Visit(visit func(name string, value interface{})) {
	if s.values != nil {
		for k, v := range s.values {
			visit(k, v)
		}
	}
}

func (s *MessageState) validateName(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("specified name is invalid")
	}
	if len(name) > MESSAGE_STATE_NAME_MAX_LENGTH {
		return fmt.Errorf("specified name is too long (max lenght: %d)", MESSAGE_STATE_NAME_MAX_LENGTH)
	}
	for i := 0; i < len(name); i++ {
		ch := name[i]
		if !(s.isValidNameChar(ch)) {
			return fmt.Errorf("specified name contains invalid '%c' at %d", ch, i)
		}
	}
	return nil
}

func (s *MessageState) isValidNameChar(ch byte) bool {
	if ch == '_' || ch == '-' ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9') {
		return true
	}
	return false
}
