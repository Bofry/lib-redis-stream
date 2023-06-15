package redis

import (
	"reflect"
	"testing"
)

func TestMessageContent_WriteTo(t *testing.T) {
	content := NewMessageContent()

	content.State.Set("foo", "bar")
	content.Values = map[string]interface{}{
		"one":   "eins",
		"two":   "zwei",
		"three": "drei",
	}

	var (
		payload map[string]interface{} = map[string]interface{}{}
	)

	content.WriteTo(payload)

	expectedPayloadSize := 4
	if expectedPayloadSize != len(payload) {
		t.Errorf("PayloadSize expected: %v, got: %v", expectedPayloadSize, len(payload))
	}
	expectedPayload := map[string]interface{}{
		"header:foo": "bar",
		"one":        "eins",
		"two":        "zwei",
		"three":      "drei",
	}
	if !reflect.DeepEqual(expectedPayload, payload) {
		t.Errorf("Payload expected: %v, got: %v", expectedPayload, payload)
	}
}

func TestDecodeMessageContent_Well(t *testing.T) {
	container := map[string]interface{}{
		"header:foo": "bar",
		"one":        "eins",
		"two":        "zwei",
		"three":      "drei",
	}

	content := DecodeMessageContent(container)

	{
		if content == nil {
			t.Fatalf("MessageContent should not nil")
		}
	}
	{
		var state map[string]interface{} = make(map[string]interface{})
		content.State.Visit(func(name string, value interface{}) {
			state[name] = value
		})
		expectedState := map[string]interface{}{
			"foo": "bar",
		}
		if !reflect.DeepEqual(expectedState, state) {
			t.Errorf("MessageContent.State() expected: %v, got: %v", expectedState, state)
		}
	}
	{
		expectedValues := map[string]interface{}{
			"one":   "eins",
			"two":   "zwei",
			"three": "drei",
		}
		if !reflect.DeepEqual(expectedValues, content.Values) {
			t.Errorf("MessageContent.Values expected: %v, got: %v", expectedValues, content.Values)
		}
	}
}

func TestDecodeMessageContent_Nil(t *testing.T) {
	var container map[string]interface{} = nil

	content := DecodeMessageContent(container)

	{
		if content != nil {
			t.Fatalf("MessageContent should be nil")
		}
	}
}

func TestMessageContent_Sanity(t *testing.T) {
	content := NewMessageContent()

	content.State.Set("foo", "bar")
	content.Values = map[string]interface{}{
		"one":   "eins",
		"two":   "zwei",
		"three": "drei",
	}

	var (
		payload map[string]interface{} = map[string]interface{}{}
	)

	content.WriteTo(payload)

	expectedPayloadSize := 4
	if expectedPayloadSize != len(payload) {
		t.Errorf("PayloadSize expected: %v, got: %v", expectedPayloadSize, len(payload))
	}
	expectedPayload := map[string]interface{}{
		"header:foo": "bar",
		"one":        "eins",
		"two":        "zwei",
		"three":      "drei",
	}
	if !reflect.DeepEqual(expectedPayload, payload) {
		t.Errorf("Payload expected: %v, got: %v", expectedPayload, payload)
	}

	out := DecodeMessageContent(payload)

	{
		if out == nil {
			t.Fatalf("MessageContent should not nil")
		}
	}
	{
		var state map[string]interface{} = make(map[string]interface{})
		out.State.Visit(func(name string, value interface{}) {
			state[name] = value
		})
		expectedState := map[string]interface{}{
			"foo": "bar",
		}
		if !reflect.DeepEqual(expectedState, state) {
			t.Errorf("MessageContent.State() expected: %v, got: %v", expectedState, state)
		}
	}
	{
		expectedValues := map[string]interface{}{
			"one":   "eins",
			"two":   "zwei",
			"three": "drei",
		}
		if !reflect.DeepEqual(expectedValues, out.Values) {
			t.Errorf("MessageContent.Values expected: %v, got: %v", expectedValues, out.Values)
		}
	}
}
