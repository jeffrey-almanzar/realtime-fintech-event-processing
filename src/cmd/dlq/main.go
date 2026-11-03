// DLQ tool: lists dead-lettered events, or replays them to the main topic
// after the underlying problem is fixed (e.g. the account was funded).
//
//	go run ./src/cmd/dlq            # list everything in the DLQ
//	go run ./src/cmd/dlq -replay    # republish each event once
//
// Replay uses its own consumer group and commits after each event, so running
// it again only replays events that arrived since. Replayed events keep their
// event_id, so anything already applied is still deduplicated.

package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/config"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/processor"
)

// idleTimeout is how long to wait for more messages before assuming the end
// of the DLQ has been reached.
const idleTimeout = 3 * time.Second

func main() {
	replay := flag.Bool("replay", false, "republish dead-lettered events to the main topic")
	flag.Parse()

	cfg := config.LoadKafka()
	if err := run(cfg, *replay); err != nil {
		log.Fatal(err)
	}
}

func run(cfg config.Kafka, replay bool) error {
	groupID := "dlq-inspect-" + uuid.NewString()[:8] // fresh group: read from the start, never commit
	if replay {
		groupID = "dlq-replayer"
	}
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.BootstrapServers,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	if err := c.Subscribe(cfg.DLQTopic, nil); err != nil {
		return err
	}

	var p *kafka.Producer
	if replay {
		if p, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": cfg.BootstrapServers, "enable.idempotence": true}); err != nil {
			return err
		}
		defer p.Close()
	}

	count := 0
	for {
		msg, err := c.ReadMessage(idleTimeout)
		if err != nil {
			var kerr kafka.Error
			if errors.As(err, &kerr) && kerr.IsTimeout() {
				break
			}
			return err
		}
		count++
		describe(msg)

		if replay {
			if err := republish(p, cfg.Topic, msg); err != nil {
				return fmt.Errorf("replay %s: %w", msg.TopicPartition, err)
			}
			if _, err := c.CommitMessage(msg); err != nil {
				return fmt.Errorf("commit %s: %w", msg.TopicPartition, err)
			}
		}
	}

	action := "found"
	if replay {
		action = "replayed to " + cfg.Topic
	}
	fmt.Printf("\n%d dead-lettered event(s) %s\n", count, action)
	return nil
}

func describe(m *kafka.Message) {
	h := map[string]string{}
	for _, header := range m.Headers {
		h[header.Key] = string(header.Value)
	}
	fmt.Printf("offset=%d reason=%s source=%s[%s]@%s failed_at=%s\n  error: %s\n",
		m.TopicPartition.Offset, h[processor.HeaderDLQReason],
		h[processor.HeaderDLQSourceTopic], h[processor.HeaderDLQSourcePartition], h[processor.HeaderDLQSourceOffset],
		h[processor.HeaderDLQFailedAt], h[processor.HeaderDLQError])

	var tx fintechv1.Transaction
	if err := proto.Unmarshal(m.Value, &tx); err == nil {
		fmt.Printf("  event: id=%s account=%s type=%s amount=%d %s\n",
			tx.EventId, tx.AccountId, tx.Type, tx.AmountMinor, tx.Currency)
	} else {
		fmt.Printf("  event: <%d undecodable bytes>\n", len(m.Value))
	}
}

// republish sends the original key, value, and headers (minus the DLQ
// metadata) back to the main topic and waits for the acknowledgement.
func republish(p *kafka.Producer, topic string, m *kafka.Message) error {
	var headers []kafka.Header
	for _, h := range m.Headers {
		if !strings.HasPrefix(h.Key, "dlq.") {
			headers = append(headers, h)
		}
	}
	delivery := make(chan kafka.Event, 1)
	err := p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            m.Key,
		Value:          m.Value,
		Headers:        headers,
	}, delivery)
	if err != nil {
		return err
	}
	if r := (<-delivery).(*kafka.Message); r.TopicPartition.Error != nil {
		return r.TopicPartition.Error
	}
	return nil
}
