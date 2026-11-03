package telemetry

import (
	"context"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestHeaderCarrierSetReplaces(t *testing.T) {
	headers := []kafka.Header{{Key: "traceparent", Value: []byte("old")}, {Key: "other", Value: []byte("x")}}
	c := HeaderCarrier{Headers: &headers}

	c.Set("traceparent", "new")

	if got := c.Get("traceparent"); got != "new" {
		t.Errorf("Get(traceparent) = %q, want %q", got, "new")
	}
	if len(headers) != 2 {
		t.Errorf("len(headers) = %d, want 2", len(headers))
	}
}

func TestHeaderCarrierRoundTrip(t *testing.T) {
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3},
		SpanID:     trace.SpanID{4, 5, 6},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	var headers []kafka.Header
	prop := propagation.TraceContext{}
	prop.Inject(ctx, HeaderCarrier{Headers: &headers})

	got := trace.SpanContextFromContext(prop.Extract(context.Background(), HeaderCarrier{Headers: &headers}))
	if got.TraceID() != sc.TraceID() || got.SpanID() != sc.SpanID() {
		t.Errorf("extracted %s/%s, want %s/%s", got.TraceID(), got.SpanID(), sc.TraceID(), sc.SpanID())
	}
}
