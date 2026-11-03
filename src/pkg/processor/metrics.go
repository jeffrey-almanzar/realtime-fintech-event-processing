package processor

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "transaction-processor"

type instruments struct {
	processed metric.Int64Counter
	retries   metric.Int64Counter
	latency   metric.Float64Histogram

	lagRegistration metric.Registration
}

func newInstruments(consumer *kafka.Consumer, groupID string) (*instruments, error) {
	meter := otel.Meter(instrumentationName)

	processed, err := meter.Int64Counter("transactions.processed",
		metric.WithDescription("Events handled by the processor, by result"),
		metric.WithUnit("{event}"))
	if err != nil {
		return nil, err
	}

	retries, err := meter.Int64Counter("transactions.retries",
		metric.WithDescription("Retries after transient errors"),
		metric.WithUnit("{retry}"))
	if err != nil {
		return nil, err
	}

	latency, err := meter.Float64Histogram("transaction.e2e.latency",
		metric.WithDescription("Time from event creation (created_at) to its balance update being committed"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 30000, 60000))
	if err != nil {
		return nil, err
	}

	lag, err := meter.Int64ObservableGauge("kafka.consumer.lag",
		metric.WithDescription("Messages between the consumer position and the end of each assigned partition"),
		metric.WithUnit("{message}"))
	if err != nil {
		return nil, err
	}
	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		return observeLag(consumer, groupID, func(v int64, opts ...metric.ObserveOption) {
			o.ObserveInt64(lag, v, opts...)
		})
	}, lag)
	if err != nil {
		return nil, err
	}

	return &instruments{processed: processed, retries: retries, latency: latency, lagRegistration: reg}, nil
}

func observeLag(c *kafka.Consumer, groupID string, observe func(int64, ...metric.ObserveOption)) error {
	assigned, err := c.Assignment()
	if err != nil || len(assigned) == 0 {
		return err
	}
	positions, err := c.Position(assigned)
	if err != nil {
		return err
	}
	for _, tp := range positions {
		if tp.Offset < 0 { // no position yet for this partition
			continue
		}
		_, high, err := c.GetWatermarkOffsets(*tp.Topic, tp.Partition)
		if err != nil {
			continue
		}
		observe(high-int64(tp.Offset), metric.WithAttributes(
			attribute.String("group", groupID),
			attribute.String("topic", *tp.Topic),
			attribute.Int("partition", int(tp.Partition)),
		))
	}
	return nil
}
