// Kafka consumer that processes financial events.
// It will evolve into the Processor in Phase 2.

package main

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/config"
)

func main() {
	cfg := config.LoadKafka()

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
		"group.id":          cfg.GroupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer func() {
		// Close commits final offsets and leaves the group cleanly.
		if err := c.Close(); err != nil {
			log.Printf("Failed to close consumer: %v", err)
		}
	}()

	if err := c.SubscribeTopics([]string{cfg.Topic}, nil); err != nil {
		log.Fatalf("Failed to subscribe to %s: %v", cfg.Topic, err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Consuming from %s as group %s", cfg.Topic, cfg.GroupID)
	for {
		select {
		case sig := <-sigs:
			log.Printf("Received %s, shutting down", sig)
			return
		default:
		}

		msg, err := c.ReadMessage(100 * time.Millisecond)
		if err != nil {
			var kerr kafka.Error
			if errors.As(err, &kerr) && kerr.IsTimeout() {
				continue
			}
			log.Printf("Consumer error: %v", err)
			continue
		}

		var tx fintechv1.Transaction
		if err := proto.Unmarshal(msg.Value, &tx); err != nil {
			log.Printf("Skipping malformed event at %s: %v", msg.TopicPartition, err)
			continue
		}
		log.Printf("Received event=%s account=%s type=%s amount=%d %s latency=%s",
			tx.EventId, tx.AccountId, tx.Type, tx.AmountMinor, tx.Currency,
			time.Since(tx.CreatedAt.AsTime()))
	}
}
