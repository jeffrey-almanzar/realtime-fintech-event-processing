package processor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/events"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/telemetry"
)

// DLQ header keys. The original message headers (including trace context)
// are kept alongside these.
const (
	HeaderDLQReason          = "dlq.reason"
	HeaderDLQError           = "dlq.error"
	HeaderDLQSourceTopic     = "dlq.source.topic"
	HeaderDLQSourcePartition = "dlq.source.partition"
	HeaderDLQSourceOffset    = "dlq.source.offset"
	HeaderDLQFailedAt        = "dlq.failed_at"
)

// outcome describes how an event was handled.
type outcome struct {
	result string // "applied", "duplicate", or "dead_lettered"
	reason string // why it was dead-lettered
}

// handle processes one message end to end and stores its offset. A non-nil
// error means the processor must stop.
func (p *Processor) handle(ctx context.Context, msg *kafka.Message) error {
	// Continue the trace started by the Event API.
	traceCtx := otel.GetTextMapPropagator().Extract(ctx, telemetry.HeaderCarrier{Headers: &msg.Headers})
	traceCtx, span := p.tracer.Start(traceCtx, *msg.TopicPartition.Topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", *msg.TopicPartition.Topic),
			attribute.Int("messaging.destination.partition.id", int(msg.TopicPartition.Partition)),
			attribute.Int64("messaging.kafka.offset", int64(msg.TopicPartition.Offset)),
		))
	defer span.End()

	out, err := p.process(traceCtx, ctx, msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	attrs := []attribute.KeyValue{attribute.String("result", out.result)}
	if out.reason != "" {
		attrs = append(attrs, attribute.String("reason", out.reason))
	}
	span.SetAttributes(attrs...)
	p.metrics.processed.Add(traceCtx, 1, metric.WithAttributes(attrs...))

	if p.cfg.AfterHandle != nil {
		if err := p.cfg.AfterHandle(msg); err != nil {
			return err
		}
	}
	if _, err := p.consumer.StoreMessage(msg); err != nil {
		return fmt.Errorf("store offset: %w", err)
	}
	return nil
}

// process applies the event or dead-letters it. Database work runs on a
// context that ignores shutdown, so an in-flight transaction always finishes;
// runCtx only interrupts retry backoff.
func (p *Processor) process(traceCtx, runCtx context.Context, msg *kafka.Message) (outcome, error) {
	workCtx := context.WithoutCancel(traceCtx)

	var tx fintechv1.Transaction
	if err := proto.Unmarshal(msg.Value, &tx); err != nil {
		return p.deadLetter(workCtx, runCtx, msg, "malformed", err)
	}
	if err := events.Validate(&tx); err != nil {
		return p.deadLetter(workCtx, runCtx, msg, "invalid", err)
	}
	delta, _ := events.SignedAmount(&tx) // checked by Validate

	var applied bool
	err := p.retry(workCtx, runCtx, "apply", func() error {
		var err error
		applied, err = p.store.Apply(workCtx, &tx, delta, msg.TopicPartition.Partition, int64(msg.TopicPartition.Offset))
		return err
	})
	var rej *Rejection
	switch {
	case errors.As(err, &rej):
		return p.deadLetter(workCtx, runCtx, msg, rej.Reason, rej)
	case err != nil && isPermanent(err):
		return p.deadLetter(workCtx, runCtx, msg, "invalid", err)
	case err != nil:
		return outcome{}, fmt.Errorf("apply event %s: %w", tx.EventId, err)
	case !applied:
		return outcome{result: "duplicate"}, nil
	}

	elapsed := float64(time.Since(tx.CreatedAt.AsTime())) / float64(time.Millisecond)
	p.metrics.latency.Record(workCtx, elapsed)
	return outcome{result: "applied"}, nil
}

// deadLetter publishes msg to the DLQ topic and waits for the broker to
// acknowledge it, so the original offset can be stored safely.
func (p *Processor) deadLetter(workCtx, runCtx context.Context, msg *kafka.Message, reason string, cause error) (outcome, error) {
	log.Printf("Dead-lettering %s (%s): %v", msg.TopicPartition, reason, cause)

	headers := append([]kafka.Header{}, msg.Headers...)
	headers = append(headers,
		kafka.Header{Key: HeaderDLQReason, Value: []byte(reason)},
		kafka.Header{Key: HeaderDLQError, Value: []byte(cause.Error())},
		kafka.Header{Key: HeaderDLQSourceTopic, Value: []byte(*msg.TopicPartition.Topic)},
		kafka.Header{Key: HeaderDLQSourcePartition, Value: []byte(strconv.Itoa(int(msg.TopicPartition.Partition)))},
		kafka.Header{Key: HeaderDLQSourceOffset, Value: []byte(strconv.FormatInt(int64(msg.TopicPartition.Offset), 10))},
		kafka.Header{Key: HeaderDLQFailedAt, Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
	)
	topic := p.cfg.Kafka.DLQTopic

	err := p.retry(workCtx, runCtx, "dead-letter", func() error {
		delivery := make(chan kafka.Event, 1)
		err := p.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            msg.Key,
			Value:          msg.Value,
			Headers:        headers,
		}, delivery)
		if err != nil {
			return err
		}
		if m := (<-delivery).(*kafka.Message); m.TopicPartition.Error != nil {
			return m.TopicPartition.Error
		}
		return nil
	})
	if err != nil {
		return outcome{}, fmt.Errorf("dead-letter %s: %w", msg.TopicPartition, err)
	}
	return outcome{result: "dead_lettered", reason: reason}, nil
}

// retry runs op until it succeeds, fails permanently, or runs out of
// attempts. Shutdown interrupts the backoff and returns errStopping.
func (p *Processor) retry(workCtx, runCtx context.Context, name string, op func() error) error {
	backoff := p.cfg.Retry.InitialBackoff
	for attempt := 1; ; attempt++ {
		err := op()
		if err == nil || isPermanent(err) || attempt >= p.cfg.Retry.MaxAttempts {
			return err
		}

		log.Printf("Retrying %s after error (attempt %d/%d, waiting %s): %v",
			name, attempt, p.cfg.Retry.MaxAttempts, backoff, err)
		p.metrics.retries.Add(workCtx, 1, metric.WithAttributes(attribute.String("operation", name)))
		select {
		case <-time.After(backoff):
		case <-runCtx.Done():
			return errStopping
		}
		backoff = min(2*backoff, p.cfg.Retry.MaxBackoff)
	}
}
