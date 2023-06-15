package redis

import (
	"context"

	"github.com/Bofry/lib-redis-stream/tracing"
	"github.com/Bofry/trace"
	"go.opentelemetry.io/otel/propagation"
)

var _ ProduceMessageOption = new(ProduceMessageContentOption)

type ProduceMessageContentOption func(msg *MessageContent) error

func (proc ProduceMessageContentOption) applyContent(msg *MessageContent) error {
	return proc(msg)
}

func (proc ProduceMessageContentOption) applyID(id string) string {
	return id
}

func WithTracePropagation(ctx context.Context, propagator propagation.TextMapPropagator) ProduceMessageContentOption {
	return func(msg *MessageContent) error {
		carrier := tracing.NewMessageStateCarrier(&msg.State)
		if propagator == nil {
			propagator = trace.GetTextMapPropagator()
		}
		propagator.Inject(ctx, carrier)
		return nil
	}
}
