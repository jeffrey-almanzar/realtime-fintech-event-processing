package workflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
)

// Publisher sends an event into the pipeline.
type Publisher interface {
	Publish(ctx context.Context, tx *fintechv1.Transaction) error
}

// OutcomeStore records how a settlement ended.
type OutcomeStore interface {
	RecordOutcome(ctx context.Context, eventID, status, detail string) error
}

// Activities holds the workflow's side effects. Temporal may run any of them
// more than once, so each one must be safe to repeat.
type Activities struct {
	Provider Provider
	Refunds  Publisher
	Outcomes OutcomeStore
}

// ChargeProvider charges the withdrawal. The event ID is the idempotency key,
// so a retry can't charge twice.
func (a *Activities) ChargeProvider(ctx context.Context, in SettlementInput) error {
	err := a.Provider.Charge(ctx, in.EventID, in.AmountMinor, in.Currency)
	if errors.Is(err, ErrDeclined) {
		return temporal.NewNonRetryableApplicationError("payment declined by provider", declinedErrorType, err)
	}
	return err
}

// PublishRefund returns the money by sending a refund through the normal
// pipeline, so it goes through the same ordering, and live updates, as any
// other event. Its ID is derived from the withdrawal's, so the Processor
// deduplicates a repeated publish and the refund applies exactly once.
func (a *Activities) PublishRefund(ctx context.Context, in SettlementInput) error {
	return a.Refunds.Publish(ctx, &fintechv1.Transaction{
		EventId:     RefundEventID(in.EventID),
		AccountId:   in.AccountID,
		Type:        fintechv1.TransactionType_TRANSACTION_TYPE_REFUND,
		AmountMinor: in.AmountMinor,
		Currency:    in.Currency,
		CreatedAt:   timestamppb.Now(),
	})
}

// RecordOutcome stores the final status, which the Query API reports.
func (a *Activities) RecordOutcome(ctx context.Context, eventID, status, detail string) error {
	return a.Outcomes.RecordOutcome(ctx, eventID, status, detail)
}

var refundNamespace = uuid.MustParse("6f1d2a9e-3b57-4c1a-9a0e-5d2c7e8f1b34")

// RefundEventID returns the same refund ID every time for a given withdrawal.
func RefundEventID(withdrawalEventID string) string {
	return uuid.NewSHA1(refundNamespace, []byte(withdrawalEventID)).String()
}

// PostgresOutcomes records settlement outcomes in Postgres.
type PostgresOutcomes struct {
	Pool      *pgxpool.Pool
	completed metric.Int64Counter
}

// NewPostgresOutcomes creates the store and its metric.
func NewPostgresOutcomes(pool *pgxpool.Pool) (*PostgresOutcomes, error) {
	completed, err := otel.Meter("settlement-worker").Int64Counter("settlements.completed",
		metric.WithDescription("Settlements finished, by outcome"),
		metric.WithUnit("{settlement}"))
	if err != nil {
		return nil, err
	}
	return &PostgresOutcomes{Pool: pool, completed: completed}, nil
}

// RecordOutcome implements OutcomeStore.
func (s *PostgresOutcomes) RecordOutcome(ctx context.Context, eventID, status, detail string) error {
	tag, err := s.Pool.Exec(ctx,
		"UPDATE settlements SET status = $2, detail = NULLIF($3, ''), updated_at = now() WHERE event_id = $1",
		eventID, status, detail)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("no settlement for event %s", eventID)
	}
	s.completed.Add(ctx, 1, metric.WithAttributes(attribute.String("status", status)))
	return nil
}

// KafkaPublisher produces events and waits for the broker's acknowledgement.
type KafkaPublisher struct {
	Producer *kafka.Producer
	Topic    string
}

// Publish implements Publisher, keyed by account like every other event.
func (p *KafkaPublisher) Publish(ctx context.Context, tx *fintechv1.Transaction) error {
	value, err := proto.Marshal(tx)
	if err != nil {
		return err
	}
	delivery := make(chan kafka.Event, 1)
	err = p.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.Topic, Partition: kafka.PartitionAny},
		Key:            []byte(tx.AccountId),
		Value:          value,
	}, delivery)
	if err != nil {
		return err
	}
	select {
	case e := <-delivery:
		return e.(*kafka.Message).TopicPartition.Error
	case <-ctx.Done():
		return ctx.Err() // the activity retries; the event ID makes that safe
	}
}
