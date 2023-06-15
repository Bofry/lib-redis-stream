package redis

import (
	"reflect"
	"testing"
)

func TestMessageState_Init(t *testing.T) {
	state := MessageState{}

	{
		size := state.Len()
		var expectedSize int = 0
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		ok := state.Has("foo")
		var expectedOk bool = false
		if expectedOk != ok {
			t.Errorf("MessageState.Has(foo) expected: %v, got: %v", expectedOk, ok)
		}
	}
}

func TestMessageState_Set(t *testing.T) {
	state := MessageState{}

	// add foo, <empty>
	{
		old, err := state.Set("foo", nil)
		var expectedErr error = nil
		if expectedErr != err {
			t.Errorf("MessageState.Set().Err expected: %v, got: %v", expectedErr, err)
		}
		var expectedOld interface{} = nil
		if !reflect.DeepEqual(expectedOld, old) {
			t.Errorf("MessageState.Set().Old expected: %v, got: %v", expectedOld, old)
		}

		size := state.Len()
		var expectedSize int = 0
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		ok := state.Has("foo")
		var expectedOk bool = false
		if expectedOk != ok {
			t.Errorf("MessageState.Has(foo) expected: %v, got: %v", expectedOk, ok)
		}
	}

	// add foo, bar
	{
		old, err := state.Set("foo", "bar")
		var expectedErr error = nil
		if expectedErr != err {
			t.Errorf("MessageState.Set() expected: %v, got: %v", expectedErr, err)
		}
		var expectedOld interface{} = nil
		if !reflect.DeepEqual(expectedOld, old) {
			t.Errorf("MessageState.Set().Old expected: %v, got: %v", expectedOld, old)
		}

		size := state.Len()
		var expectedSize int = 1
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		ok := state.Has("foo")
		var expectedOk bool = true
		if expectedOk != ok {
			t.Errorf("MessageState.Has(foo) expected: %v, got: %v", expectedOk, ok)
		}
	}

	// add foo2, bar2
	{
		old, err := state.Set("foo2", "bar2")
		var expectedErr error = nil
		if expectedErr != err {
			t.Errorf("MessageState.Set() expected: %v, got: %v", expectedErr, err)
		}
		var expectedOld interface{} = nil
		if !reflect.DeepEqual(expectedOld, old) {
			t.Errorf("MessageState.Set().Old expected: %v, got: %v", expectedOld, old)
		}

		size := state.Len()
		var expectedSize int = 2
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		ok := state.Has("foo2")
		var expectedOk bool = true
		if expectedOk != ok {
			t.Errorf("MessageState.Has(foo) expected: %v, got: %v", expectedOk, ok)
		}
	}

	// add baz, <empty>
	{
		old, err := state.Set("baz", nil)
		var expectedErr error = nil
		if expectedErr != err {
			t.Errorf("MessageState.Set() expected: %v, got: %v", expectedErr, err)
		}
		var expectedOld interface{} = nil
		if !reflect.DeepEqual(expectedOld, old) {
			t.Errorf("MessageState.Set().Old expected: %v, got: %v", expectedOld, old)
		}

		size := state.Len()
		var expectedSize int = 2
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		ok := state.Has("baz")
		var expectedOk bool = false
		if expectedOk != ok {
			t.Errorf("MessageState.Has(foo) expected: %v, got: %v", expectedOk, ok)
		}
	}

	// replace foo2, <empty>. It same as Del().
	{
		old, err := state.Set("foo2", nil)
		var expectedErr error = nil
		if expectedErr != err {
			t.Errorf("MessageState.Set() expected: %v, got: %v", expectedErr, err)
		}
		var expectedOld interface{} = "bar2"
		if !reflect.DeepEqual(expectedOld, old) {
			t.Errorf("MessageState.Set().Old expected: %v, got: %v", expectedOld, old)
		}

		size := state.Len()
		var expectedSize int = 1
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		ok := state.Has("foo2")
		var expectedOk bool = false
		if expectedOk != ok {
			t.Errorf("MessageState.Has(foo) expected: %v, got: %v", expectedOk, ok)
		}
	}

	// re-add foo2, bar2
	{
		old, err := state.Set("foo2", "bar2")
		var expectedErr error = nil
		if expectedErr != err {
			t.Errorf("MessageState.Set() expected: %v, got: %v", expectedErr, err)
		}
		var expectedOld interface{} = nil
		if !reflect.DeepEqual(expectedOld, old) {
			t.Errorf("MessageState.Set().Old expected: %v, got: %v", expectedOld, old)
		}

		size := state.Len()
		var expectedSize int = 2
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		ok := state.Has("foo2")
		var expectedOk bool = true
		if expectedOk != ok {
			t.Errorf("MessageState.Has(foo) expected: %v, got: %v", expectedOk, ok)
		}
	}
}

func TestMessageState_Del(t *testing.T) {
	state := MessageState{}

	// delete missing key foo
	{
		size := state.Len()
		var expectedSize int = 0
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		ok := state.Has("foo")
		var expectedOk bool = false
		if expectedOk != ok {
			t.Errorf("MessageState.Has(foo) expected: %v, got: %v", expectedOk, ok)
		}

		old := state.Del("foo")
		var expectedOld interface{} = nil
		if !reflect.DeepEqual(expectedOld, old) {
			t.Errorf("MessageState.Del(foo).Old expected: %v, got: %v", expectedOld, old)
		}
	}

	// delete key foo
	{
		state.Set("foo", []byte("bar"))
		size := state.Len()
		var expectedSize int = 1
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		old := state.Del("foo")
		var expectedOld []byte = []byte("bar")
		if !reflect.DeepEqual(expectedOld, old) {
			t.Errorf("MessageState.Del().Old expected: %v, got: %v", expectedOld, old)
		}

		size = state.Len()
		expectedSize = 0
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		ok := state.Has("foo")
		var expectedOk bool = false
		if expectedOk != ok {
			t.Errorf("MessageState.Has(foo) expected: %v, got: %v", expectedOk, ok)
		}
	}
}

func TestMessageState_Value(t *testing.T) {
	state := MessageState{}

	// get missing key foo
	{
		size := state.Len()
		var expectedSize int = 0
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		ok := state.Has("foo")
		var expectedOk bool = false
		if expectedOk != ok {
			t.Errorf("MessageState.Has(foo) expected: %v, got: %v", expectedOk, ok)
		}

		value := state.Value("foo")
		var expectedValue interface{} = nil
		if !reflect.DeepEqual(expectedValue, value) {
			t.Errorf("MessageState.Value(foo) expected: %v, got: %v", expectedValue, value)
		}
	}

	// get foo
	{
		state.Set("foo", "bar")
		size := state.Len()
		var expectedSize int = 1
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		value := state.Value("foo")
		var expectedValue interface{} = "bar"
		if !reflect.DeepEqual(expectedValue, value) {
			t.Errorf("MessageState.Value(foo) expected: %v, got: %v", expectedValue, value)
		}
	}

	// get foo after value has been updated
	{
		state.Set("foo", "bar2")
		size := state.Len()
		var expectedSize int = 1
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		value := state.Value("foo")
		var expectedValue interface{} = "bar2"
		if !reflect.DeepEqual(expectedValue, value) {
			t.Errorf("MessageState.Value(foo) expected: %v, got: %v", expectedValue, value)
		}
	}
}

func TestMessageState_Visit(t *testing.T) {
	state := MessageState{}

	// visit empty MessageState
	{
		size := state.Len()
		var expectedSize int = 0
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		ok := state.Has("foo")
		var expectedOk bool = false
		if expectedOk != ok {
			t.Errorf("MessageState.Has(foo) expected: %v, got: %v", expectedOk, ok)
		}

		var values map[string]interface{} = make(map[string]interface{})
		state.Visit(func(name string, value interface{}) {
			values[name] = value
		})
		var expectedValues map[string]interface{} = map[string]interface{}{}
		if !reflect.DeepEqual(expectedValues, values) {
			t.Errorf("MessageState.Visit() expected: %v, got: %v", expectedValues, values)
		}
	}

	{
		state.Set("foo", "bar")
		size := state.Len()
		var expectedSize int = 1
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		var values map[string]interface{} = make(map[string]interface{})
		state.Visit(func(name string, value interface{}) {
			values[name] = value
		})
		var expectedValues map[string]interface{} = map[string]interface{}{
			"foo": "bar",
		}
		if !reflect.DeepEqual(expectedValues, values) {
			t.Errorf("MessageState.Visit() expected: %v, got: %v", expectedValues, values)
		}
	}

	{
		state.Set("foo", "bar2")
		size := state.Len()
		var expectedSize int = 1
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		var values map[string]interface{} = make(map[string]interface{})
		state.Visit(func(name string, value interface{}) {
			values[name] = value
		})
		var expectedValues map[string]interface{} = map[string]interface{}{
			"foo": "bar2",
		}
		if !reflect.DeepEqual(expectedValues, values) {
			t.Errorf("MessageState.Visit() expected: %v, got: %v", expectedValues, values)
		}
	}

	{
		state.Set("foo", nil)
		size := state.Len()
		var expectedSize int = 0
		if expectedSize != size {
			t.Errorf("MessageState.Len() expected: %v, got: %v", expectedSize, size)
		}

		var values map[string]interface{} = make(map[string]interface{})
		state.Visit(func(name string, value interface{}) {
			values[name] = value
		})
		var expectedValues map[string]interface{} = map[string]interface{}{}
		if !reflect.DeepEqual(expectedValues, values) {
			t.Errorf("MessageState.Visit() expected: %v, got: %v", expectedValues, values)
		}
	}
}
