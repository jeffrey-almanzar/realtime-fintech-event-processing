// Kafka producer that generates simulated financial events.
// It will evolve into the Event API in Phase 2.

package main

import (
	"fmt"
	"log"
	"math/rand/v2"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/config"
)

const numEvents = 10

func main() {
	cfg := config.LoadKafka()

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.BootstrapServers,
		"enable.idempotence": true,
		"acks":               "all",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	deliveries := make(chan kafka.Event, numEvents)
	for i := range numEvents {
		tx := &fintechv1.Transaction{
			EventId:     uuid.NewString(),
			AccountId:   fmt.Sprintf("acct-%d", i%3),
			Type:        fintechv1.TransactionType_TRANSACTION_TYPE_DEPOSIT,
			AmountMinor: rand.Int64N(100_000) + 1,
			Currency:    "USD",
			CreatedAt:   timestamppb.Now(),
		}
		value, err := proto.Marshal(tx)
		if err != nil {
			log.Fatalf("Failed to marshal event: %v", err)
		}

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &cfg.Topic, Partition: kafka.PartitionAny},
			Key:            []byte(tx.AccountId),
			Value:          value,
		}, deliveries)
		if err != nil {
			log.Fatalf("Failed to enqueue event %s: %v", tx.EventId, err)
		}
	}

	failed := 0
	for range numEvents {
		m := (<-deliveries).(*kafka.Message)
		if m.TopicPartition.Error != nil {
			failed++
			log.Printf("Delivery failed: %v", m.TopicPartition.Error)
			continue
		}
		log.Printf("Delivered key=%s to %s", m.Key, m.TopicPartition)
	}
	if failed > 0 {
		log.Fatalf("%d of %d events failed to deliver", failed, numEvents)
	}
}
