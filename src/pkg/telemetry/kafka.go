package telemetry

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/propagation"
)

// HeaderCarrier adapts Kafka message headers to an OpenTelemetry
// TextMapCarrier, so trace context can travel from producer to consumer.
type HeaderCarrier struct {
	Headers *[]kafka.Header
}

var _ propagation.TextMapCarrier = HeaderCarrier{}

// Get returns the value of the first header with the given key.
func (c HeaderCarrier) Get(key string) string {
	for _, h := range *c.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set replaces any existing header with the given key.
func (c HeaderCarrier) Set(key, value string) {
	kept := (*c.Headers)[:0]
	for _, h := range *c.Headers {
		if h.Key != key {
			kept = append(kept, h)
		}
	}
	*c.Headers = append(kept, kafka.Header{Key: key, Value: []byte(value)})
}

// Keys lists all header keys.
func (c HeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.Headers))
	for _, h := range *c.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}
