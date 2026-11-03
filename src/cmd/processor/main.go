// Processor: consumes financial events from Kafka and applies them to account
// balances in Postgres. Offsets are committed only after the database write
// succeeds, so a crash causes a reprocess (deduplicated by event_id), never a
// lost event.
//
// This is the Phase 2 baseline: one goroutine processes messages one at a
// time. Retries, the DLQ, and per-partition concurrency come in Phase 3.

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/config"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/telemetry"
)

const serviceName = "transaction-processor"

func main() {
	if err := run(); err != nil {
		log.Printf("Processor stopped: %v", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTelemetry, err := telemetry.Init(ctx, serviceName)
	if err != nil {
		return fmt.Errorf("initialize telemetry: %w", err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTelemetry(flushCtx); err != nil {
			log.Printf("Telemetry shutdown error: %v", err)
		}
	}()

	store, err := NewStore(ctx, config.DatabaseURL())
	if err != nil {
		return fmt.Errorf("connect to Postgres: %w", err)
	}
	defer store.Close()

	cfg := config.LoadKafka()
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
		"group.id":          cfg.GroupID,
		"auto.offset.reset": "earliest",
		// Offsets are stored manually after each successful write; the
		// background auto-commit only ever commits those stored offsets.
		"enable.auto.offset.store": false,
	})
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	defer func() {
		// Close commits the final stored offsets and leaves the group.
		if err := consumer.Close(); err != nil {
			log.Printf("Failed to close consumer: %v", err)
		}
	}()

	if err := consumer.SubscribeTopics([]string{cfg.Topic}, nil); err != nil {
		return fmt.Errorf("subscribe to %s: %w", cfg.Topic, err)
	}

	p, err := newProcessor(consumer, store, cfg.GroupID)
	if err != nil {
		return err
	}
	// Runs before consumer.Close, so the final metric collection on shutdown
	// doesn't query a closed consumer.
	defer func() {
		if err := p.lagRegistration.Unregister(); err != nil {
			log.Printf("Failed to unregister lag callback: %v", err)
		}
	}()

	log.Printf("Consuming from %s as group %s", cfg.Topic, cfg.GroupID)
	for ctx.Err() == nil {
		msg, err := consumer.ReadMessage(100 * time.Millisecond)
		if err != nil {
			var kerr kafka.Error
			if errors.As(err, &kerr) && kerr.IsTimeout() {
				continue
			}
			log.Printf("Consumer error: %v", err)
			continue
		}
		// Fail-stop on database errors: the offset isn't stored, so after a
		// restart the event is read again.
		if err := p.handle(ctx, msg); err != nil {
			return err
		}
	}
	log.Print("Shutting down")
	return nil
}

type processor struct {
	consumer  *kafka.Consumer
	store     *Store
	tracer    trace.Tracer
	processed metric.Int64Counter
	latency   metric.Float64Histogram

	lagRegistration metric.Registration
}

func newProcessor(consumer *kafka.Consumer, store *Store, groupID string) (*processor, error) {
	meter := otel.Meter(serviceName)

	processed, err := meter.Int64Counter("transactions.processed",
		metric.WithDescription("Events handled by the processor, by result"),
		metric.WithUnit("{event}"))
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

	return &processor{
		consumer:        consumer,
		store:           store,
		tracer:          otel.Tracer(serviceName),
		processed:       processed,
		latency:         latency,
		lagRegistration: reg,
	}, nil
}

func (p *processor) handle(ctx context.Context, msg *kafka.Message) error {
	// Continue the trace started by the Event API.
	ctx = otel.GetTextMapPropagator().Extract(ctx, telemetry.HeaderCarrier{Headers: &msg.Headers})
	ctx, span := p.tracer.Start(ctx, *msg.TopicPartition.Topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", *msg.TopicPartition.Topic),
			attribute.Int("messaging.destination.partition.id", int(msg.TopicPartition.Partition)),
			attribute.Int64("messaging.kafka.offset", int64(msg.TopicPartition.Offset)),
		))
	defer span.End()

	result, err := p.apply(ctx, msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetAttributes(attribute.String("result", result))
	p.processed.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))

	if _, err := p.consumer.StoreMessage(msg); err != nil {
		return fmt.Errorf("store offset: %w", err)
	}
	return nil
}

// apply returns "applied", "duplicate", or "malformed".
func (p *processor) apply(ctx context.Context, msg *kafka.Message) (string, error) {
	var tx fintechv1.Transaction
	if err := proto.Unmarshal(msg.Value, &tx); err != nil {
		// Skipped for now; Phase 3 routes these to the DLQ.
		log.Printf("Skipping malformed event at %s: %v", msg.TopicPartition, err)
		return "malformed", nil
	}
	delta, err := signedAmount(&tx)
	if err != nil || tx.EventId == "" || tx.CreatedAt == nil {
		log.Printf("Skipping invalid event at %s: %v", msg.TopicPartition, &tx)
		return "malformed", nil
	}

	applied, err := p.store.Apply(ctx, &tx, delta, msg.TopicPartition.Partition, int64(msg.TopicPartition.Offset))
	if err != nil {
		return "", fmt.Errorf("apply event %s: %w", tx.EventId, err)
	}
	if !applied {
		return "duplicate", nil
	}

	elapsed := float64(time.Since(tx.CreatedAt.AsTime())) / float64(time.Millisecond)
	p.latency.Record(ctx, elapsed)
	return "applied", nil
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
