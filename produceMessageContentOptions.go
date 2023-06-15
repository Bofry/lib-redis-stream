package redis

import (
	"context"

	"github.com/Bofry/lib-redis-stream/tracing"
	"github.com/Bofry/trace"
	"go.opentelemetry.io/otel/propagation"
)

var _ ProduceMessageContentOption = new(ProduceMessageContentOptionProc)

type ProduceMessageContentOptionProc func(msg *MessageContent) error

func (proc ProduceMessageContentOptionProc) apply(msg *MessageContent) error {
	return proc(msg)
}

func WithTracePropagation(ctx context.Context, propagator propagation.TextMapPropagator) ProduceMessageContentOptionProc {
	return func(msg *MessageContent) error {
		carrier := tracing.NewMessageStateCarrier(&msg.State)
		if propagator == nil {
			propagator = trace.GetTextMapPropagator()
		}
		propagator.Inject(ctx, carrier)
		return nil
	}
}
