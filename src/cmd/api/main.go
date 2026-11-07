// Event API: accepts financial events over HTTP and publishes them to Kafka,
// keyed by account_id so each account's events stay ordered.

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/config"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/telemetry"
)

const serviceName = "event-api"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTelemetry, err := telemetry.Init(ctx, serviceName)
	if err != nil {
		log.Fatalf("Failed to initialize telemetry: %v", err)
	}
	defer flush(shutdownTelemetry)

	cfg := config.LoadKafka()
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.BootstrapServers,
		"enable.idempotence": true,
		"acks":               "all",
		"linger.ms":          5,
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()
	defer producer.Flush(5000)

	mux := http.NewServeMux()
	mux.Handle("POST /transactions", handleCreateTransaction(&kafkaPublisher{producer: producer, topic: cfg.Topic}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	srv := &http.Server{
		Addr:              config.HTTPAddr(),
		Handler:           otelhttp.NewHandler(withCORS(mux, config.CORSAllowedOrigins()), "http.server", otelhttp.WithSpanNameFormatter(spanName)),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		log.Print("Shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP shutdown error: %v", err)
		}
	}()

	log.Printf("Event API listening on %s, publishing to %s", srv.Addr, cfg.Topic)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HTTP server error: %v", err)
	}
}

func spanName(_ string, r *http.Request) string {
	return r.Method + " " + r.URL.Path
}

func flush(shutdown func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		log.Printf("Telemetry shutdown error: %v", err)
	}
}

// kafkaPublisher produces events and waits for the broker's acknowledgement.
type kafkaPublisher struct {
	producer *kafka.Producer
	topic    string
}

func (p *kafkaPublisher) Publish(ctx context.Context, tx *fintechv1.Transaction) error {
	ctx, span := otel.Tracer(serviceName).Start(ctx, p.topic+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", p.topic),
			attribute.String("messaging.message.id", tx.EventId),
		))
	defer span.End()

	value, err := proto.Marshal(tx)
	if err != nil {
		return fail(span, fmt.Errorf("marshal event: %w", err))
	}

	// Trace context rides in the message headers so the processor's spans
	// join the same trace.
	var headers []kafka.Header
	otel.GetTextMapPropagator().Inject(ctx, telemetry.HeaderCarrier{Headers: &headers})

	delivery := make(chan kafka.Event, 1)
	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Key:            []byte(tx.AccountId),
		Value:          value,
		Headers:        headers,
	}, delivery)
	if err != nil {
		return fail(span, fmt.Errorf("enqueue event: %w", err))
	}

	select {
	case e := <-delivery:
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			return fail(span, fmt.Errorf("delivery failed: %w", m.TopicPartition.Error))
		}
		span.SetAttributes(
			attribute.Int("messaging.destination.partition.id", int(m.TopicPartition.Partition)),
			attribute.Int64("messaging.kafka.offset", int64(m.TopicPartition.Offset)),
		)
		return nil
	case <-ctx.Done():
		// The message may still be delivered; clients retry with the same
		// event_id and the processor deduplicates.
		return fail(span, ctx.Err())
	}
}

func fail(span trace.Span, err error) error {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	return err
}
