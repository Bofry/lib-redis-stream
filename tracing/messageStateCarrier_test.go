package tracing

import (
	"context"
	"reflect"
	"testing"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slices"
)

var (
	traceIDStr = "4bf92f3577b34da6a3ce929d0e0e4736"
	spanIDStr  = "00f067aa0ba902b7"

	__TEST_TRACE_ID = mustTraceIDFromHex(traceIDStr)
	__TEST_SPAN_ID  = mustSpanIDFromHex(spanIDStr)

	__TEST_PROPAGATOR = propagation.TraceContext{}
	__TEST_CONTEXT    = mustSpanContext()
)

func mustTraceIDFromHex(s string) (t trace.TraceID) {
	var err error
	t, err = trace.TraceIDFromHex(s)
	if err != nil {
		panic(err)
	}
	return
}

func mustSpanIDFromHex(s string) (t trace.SpanID) {
	var err error
	t, err = trace.SpanIDFromHex(s)
	if err != nil {
		panic(err)
	}
	return
}

func mustSpanContext() context.Context {
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    __TEST_TRACE_ID,
		SpanID:     __TEST_SPAN_ID,
		TraceFlags: 0,
	})
	return trace.ContextWithSpanContext(context.Background(), sc)
}

var _ MessageState = mockMessageState{}

type mockMessageState map[string]interface{}

func (s mockMessageState) Len() int {
	return len(s)
}
func (s mockMessageState) Has(name string) (ok bool) {
	_, ok = s[name]
	return
}
func (s mockMessageState) Del(name string) (old interface{}) {
	old, _ = s[name]
	delete(s, name)
	return
}
func (s mockMessageState) Set(name string, value interface{}) (old interface{}, err error) {
	old, _ = s[name]
	s[name] = value
	return
}
func (s mockMessageState) Value(name string) interface{} {
	return s[name]
}
func (s mockMessageState) Visit(visit func(name string, value interface{})) {
	for k, v := range s {
		visit(k, v)
	}
}

func TestMessageStateCarrier(t *testing.T) {
	var (
		state      MessageState = make(mockMessageState)
		propagator              = propagation.TraceContext{}
	)

	carrier := NewMessageStateCarrier(state)

	// inject
	{
		propagator.Inject(__TEST_CONTEXT, carrier)

		traceparent := state.Value("traceparent")
		v, ok := traceparent.(string)
		if !ok {
			t.Error("invalid MessageState 'traceparent'")
		}
		if len(v) == 0 {
			t.Error("missing MessageState 'traceparent'")
		}
	}

	// fields
	{
		keys := carrier.Keys()
		if !slices.Contains(keys, "traceparent") {
			t.Error("missing MessageState 'traceparent'")
		}
	}

	// extract
	{
		ctx := propagator.Extract(context.Background(), carrier)
		sc := trace.SpanContextFromContext(ctx)
		var expectedTraceID = __TEST_TRACE_ID
		if !reflect.DeepEqual(expectedTraceID, sc.TraceID()) {
			t.Errorf("TRACE ID expect: %v, got: %v", expectedTraceID, sc.TraceID())
		}
		var expectedSpanID = __TEST_SPAN_ID
		if !reflect.DeepEqual(expectedSpanID, sc.SpanID()) {
			t.Errorf("SPAN ID expect: %v, got: %v", expectedSpanID, sc.SpanID())
		}
	}
}
