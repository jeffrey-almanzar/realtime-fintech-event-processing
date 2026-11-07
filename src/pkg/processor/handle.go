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

// outcome describes how one message was handled.
type outcome struct {
	result    string // "applied", "duplicate", or "dead_lettered"
	reason    string // why it was dead-lettered
	cause     error
	createdAt time.Time
	published bool // already acknowledged by the DLQ topic
}

func (o *outcome) deadLetter(reason string, cause error) {
	o.result, o.reason, o.cause = "dead_lettered", reason, cause
}

// handleBatch processes messages from one partition, in order, and stores the
// offset of the last one handled. A non-nil error means the processor must stop.
func (p *Processor) handleBatch(ctx context.Context, msgs []*kafka.Message) error {
	started := time.Now()
	// Each event continues the trace started by the Event API. The database
	// work is shared, so it hangs off the first event's trace and the batch
	// span links to the others.
	topic := *msgs[0].TopicPartition.Topic
	spans := make([]trace.Span, len(msgs))
	links := make([]trace.Link, len(msgs))
	var firstCtx context.Context
	for i, msg := range msgs {
		eventCtx := otel.GetTextMapPropagator().Extract(ctx, telemetry.HeaderCarrier{Headers: &msg.Headers})
		eventCtx, spans[i] = p.tracer.Start(eventCtx, topic+" process",
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.String("messaging.system", "kafka"),
				attribute.String("messaging.destination.name", topic),
				attribute.Int("messaging.destination.partition.id", int(msg.TopicPartition.Partition)),
				attribute.Int64("messaging.kafka.offset", int64(msg.TopicPartition.Offset)),
			))
		links[i] = trace.LinkFromContext(eventCtx)
		if i == 0 {
			firstCtx = eventCtx
		}
	}
	batchCtx, batchSpan := p.tracer.Start(firstCtx, topic+" apply batch",
		trace.WithLinks(links[1:]...),
		trace.WithAttributes(attribute.Int("messaging.batch.message_count", len(msgs))))
	defer func() {
		batchSpan.End()
		for _, s := range spans {
			s.End()
		}
	}()
	p.metrics.batchSize.Record(ctx, int64(len(msgs)))
	p.buffered.Add(-int64(len(msgs)))

	outs, err := p.process(batchCtx, ctx, msgs)
	if err != nil {
		batchSpan.RecordError(err)
		batchSpan.SetStatus(codes.Error, err.Error())
		return err
	}

	p.record(ctx, msgs, outs, spans, started)

	// Stop at the first injected failure, as a crash would: offsets of events
	// handled but not yet stored are redelivered and deduplicated.
	last := -1
	var hookErr error
	if p.cfg.AfterHandle != nil {
		for i, msg := range msgs {
			if hookErr = p.cfg.AfterHandle(msg); hookErr != nil {
				break
			}
			last = i
		}
	} else {
		last = len(msgs) - 1
	}
	if last >= 0 {
		if _, err := p.consumer.StoreMessage(msgs[last]); err != nil {
			return fmt.Errorf("store offset: %w", err)
		}
	}
	return hookErr
}

func (p *Processor) record(ctx context.Context, msgs []*kafka.Message, outs []outcome, spans []trace.Span, started time.Time) {
	now := time.Now()
	stage := func(name string, d time.Duration) {
		p.metrics.stage.Record(ctx, float64(d)/float64(time.Millisecond),
			metric.WithAttributeSet(attribute.NewSet(attribute.String("stage", name))))
	}
	counts := map[[2]string]int64{}
	for i := range outs {
		o := &outs[i]
		counts[[2]string{o.result, o.reason}]++
		spans[i].SetAttributes(attribute.String("result", o.result))
		if o.reason != "" {
			spans[i].SetAttributes(attribute.String("reason", o.reason))
		}
		if o.result == "applied" {
			p.metrics.latency.Record(ctx, float64(now.Sub(o.createdAt))/float64(time.Millisecond))
			if fetched, ok := msgs[i].Opaque.(time.Time); ok {
				stage("kafka", fetched.Sub(o.createdAt))
				stage("queue", started.Sub(fetched))
				stage("apply", now.Sub(started))
			}
		}
	}
	for key, n := range counts {
		attrs := []attribute.KeyValue{attribute.String("result", key[0])}
		if key[1] != "" {
			attrs = append(attrs, attribute.String("reason", key[1]))
		}
		p.metrics.processed.Add(ctx, n, metric.WithAttributes(attrs...))
	}
}

// process decides every message and returns once the results are durable.
// Database work runs on a context that ignores shutdown, so an in-flight
// transaction always finishes; runCtx only interrupts retry backoff.
func (p *Processor) process(traceCtx, runCtx context.Context, msgs []*kafka.Message) ([]outcome, error) {
	workCtx := context.WithoutCancel(traceCtx)
	outs := make([]outcome, len(msgs))
	items := make([]BatchItem, 0, len(msgs))
	indexes := make([]int, 0, len(msgs)) // items[i] came from msgs[indexes[i]]

	for i, msg := range msgs {
		var tx fintechv1.Transaction
		if err := proto.Unmarshal(msg.Value, &tx); err != nil {
			outs[i].deadLetter("malformed", err)
			continue
		}
		if err := events.Validate(&tx); err != nil {
			outs[i].deadLetter("invalid", err)
			continue
		}
		delta, _ := events.SignedAmount(&tx) // checked by Validate
		outs[i].createdAt = tx.CreatedAt.AsTime()
		items = append(items, BatchItem{
			Tx: &tx, Delta: delta,
			Partition: msg.TopicPartition.Partition,
			Offset:    int64(msg.TopicPartition.Offset),
		})
		indexes = append(indexes, i)
	}

	err := p.applyItems(workCtx, runCtx, msgs, outs, items, indexes)
	if err == nil {
		return outs, nil
	}
	if !isPermanent(err) {
		return nil, err
	}

	// One event's contents made the database refuse the whole batch. Apply
	// them one at a time so only that event goes to the DLQ.
	for i := range items {
		single, index := items[i:i+1], indexes[i:i+1]
		if outs[index[0]].published {
			continue
		}
		err := p.applyItems(workCtx, runCtx, msgs, outs, single, index)
		if err == nil {
			continue
		}
		if !isPermanent(err) {
			return nil, err
		}
		outs[index[0]].deadLetter("invalid", err)
		if err := p.publishDeadLetters(workCtx, runCtx, msgs, outs); err != nil {
			return nil, err
		}
	}
	return outs, nil
}

// applyItems applies items (retrying transient errors) and publishes every
// pending dead letter before the transaction commits.
func (p *Processor) applyItems(workCtx, runCtx context.Context, msgs []*kafka.Message, outs []outcome, items []BatchItem, indexes []int) error {
	if len(items) == 0 {
		return p.publishDeadLetters(workCtx, runCtx, msgs, outs)
	}
	return p.retry(workCtx, runCtx, "apply", func() error {
		_, err := p.store.ApplyBatch(workCtx, items, func(results []ItemResult) error {
			for i, r := range results {
				o := &outs[indexes[i]]
				switch {
				case r.Rejection != nil:
					o.deadLetter(r.Rejection.Reason, r.Rejection)
				case r.Applied:
					o.result = "applied"
				default:
					o.result = "duplicate"
				}
			}
			return p.sendDeadLetters(msgs, outs)
		})
		return err
	})
}

func (p *Processor) publishDeadLetters(workCtx, runCtx context.Context, msgs []*kafka.Message, outs []outcome) error {
	return p.retry(workCtx, runCtx, "dead-letter", func() error {
		return p.sendDeadLetters(msgs, outs)
	})
}

// sendDeadLetters publishes every unpublished dead letter and waits for the
// broker to acknowledge them. Across a retry only the ones still unacknowledged
// are sent again, though a crash can still repeat one; replays deduplicate.
func (p *Processor) sendDeadLetters(msgs []*kafka.Message, outs []outcome) error {
	topic := p.cfg.Kafka.DLQTopic
	delivery := make(chan kafka.Event, len(msgs))
	inFlight := 0

	for i := range outs {
		o := &outs[i]
		if o.reason == "" || o.published {
			continue
		}
		msg := msgs[i]
		log.Printf("Dead-lettering %s (%s): %v", msg.TopicPartition, o.reason, o.cause)
		headers := append([]kafka.Header{}, msg.Headers...)
		headers = append(headers,
			kafka.Header{Key: HeaderDLQReason, Value: []byte(o.reason)},
			kafka.Header{Key: HeaderDLQError, Value: []byte(o.cause.Error())},
			kafka.Header{Key: HeaderDLQSourceTopic, Value: []byte(*msg.TopicPartition.Topic)},
			kafka.Header{Key: HeaderDLQSourcePartition, Value: []byte(strconv.Itoa(int(msg.TopicPartition.Partition)))},
			kafka.Header{Key: HeaderDLQSourceOffset, Value: []byte(strconv.FormatInt(int64(msg.TopicPartition.Offset), 10))},
			kafka.Header{Key: HeaderDLQFailedAt, Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		)
		dead := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            msg.Key,
			Value:          msg.Value,
			Headers:        headers,
			Opaque:         i,
		}
		if err := p.producer.Produce(dead, delivery); err != nil {
			// Still collect what's in flight, so those aren't sent twice.
			return errors.Join(err, awaitDeliveries(delivery, inFlight, outs))
		}
		inFlight++
	}
	return awaitDeliveries(delivery, inFlight, outs)
}

func awaitDeliveries(delivery <-chan kafka.Event, n int, outs []outcome) error {
	var errs []error
	for range n {
		m := (<-delivery).(*kafka.Message)
		if m.TopicPartition.Error != nil {
			errs = append(errs, m.TopicPartition.Error)
			continue
		}
		outs[m.Opaque.(int)].published = true
	}
	return errors.Join(errs...)
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
