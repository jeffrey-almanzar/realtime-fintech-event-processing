// Package processor consumes financial events from Kafka and applies them to
// account balances in Postgres.
//
// Each assigned partition gets its own worker goroutine, so events for one
// account (same key, same partition) are applied in order while partitions
// proceed in parallel. Offsets are stored only after an event is fully
// handled (applied, deduplicated, or dead-lettered), so a crash causes a
// reprocess, never a lost event.
package processor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/config"
)

// Config controls a Processor.
type Config struct {
	Kafka       config.Kafka
	DatabaseURL string
	Retry       RetryPolicy
	// PartitionBuffer is how many fetched messages may queue per partition.
	PartitionBuffer int
	// ConsumerOverrides are extra librdkafka settings (used by tests).
	ConsumerOverrides kafka.ConfigMap
	// AfterHandle, if set, runs after an event is handled and before its
	// offset is stored. Returning an error stops the processor as if it had
	// crashed at that point. Tests use it to inject failures.
	AfterHandle func(*kafka.Message) error
}

// RetryPolicy bounds retries of transient errors with exponential backoff.
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// DefaultConfig returns settings for the local stack.
func DefaultConfig() Config {
	return Config{
		Kafka:           config.LoadKafka(),
		DatabaseURL:     config.DatabaseURL(),
		Retry:           RetryPolicy{MaxAttempts: 6, InitialBackoff: 100 * time.Millisecond, MaxBackoff: 5 * time.Second},
		PartitionBuffer: 500,
	}
}

// errStopping means a worker gave up on an event because of shutdown; the
// event's offset isn't stored, so it is processed again after a restart.
var errStopping = errors.New("processor is stopping")

// Processor owns a Kafka consumer, a DLQ producer, and a Postgres store.
type Processor struct {
	cfg      Config
	consumer *kafka.Consumer
	producer *kafka.Producer
	store    *Store
	tracer   trace.Tracer
	metrics  *instruments

	// Only touched from the polling goroutine (rebalance callbacks run
	// inside Poll), so no lock is needed.
	workers map[int32]*worker
	errs    chan error
}

type worker struct {
	msgs chan *kafka.Message
	stop chan struct{}
	done chan struct{}
}

// New connects to Kafka and Postgres. Call Run to start processing.
func New(ctx context.Context, cfg Config) (*Processor, error) {
	store, err := NewStore(ctx, cfg.DatabaseURL, 16)
	if err != nil {
		return nil, fmt.Errorf("connect to Postgres: %w", err)
	}

	consumerCfg := kafka.ConfigMap{
		"bootstrap.servers": cfg.Kafka.BootstrapServers,
		"group.id":          cfg.Kafka.GroupID,
		"auto.offset.reset": "earliest",
		// Offsets are stored manually after each event is handled; the
		// background auto-commit only ever commits those stored offsets.
		"enable.auto.offset.store": false,
		// A consumer that dies without leaving the group (kill -9, OOM) holds
		// its partitions until this expires. The default is 45s.
		"session.timeout.ms":    10000,
		"heartbeat.interval.ms": 3000,
	}
	if cfg.Kafka.GroupInstanceID != "" {
		consumerCfg["group.instance.id"] = cfg.Kafka.GroupInstanceID
	}
	for k, v := range cfg.ConsumerOverrides {
		consumerCfg[k] = v
	}
	consumer, err := kafka.NewConsumer(&consumerCfg)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.Kafka.BootstrapServers,
		"enable.idempotence": true,
		"acks":               "all",
	})
	if err != nil {
		_ = consumer.Close()
		store.Close()
		return nil, fmt.Errorf("create DLQ producer: %w", err)
	}

	metrics, err := newInstruments(consumer, cfg.Kafka.GroupID)
	if err != nil {
		producer.Close()
		_ = consumer.Close()
		store.Close()
		return nil, err
	}

	return &Processor{
		cfg:      cfg,
		consumer: consumer,
		producer: producer,
		store:    store,
		tracer:   otel.Tracer(instrumentationName),
		metrics:  metrics,
		workers:  make(map[int32]*worker),
		errs:     make(chan error, 1),
	}, nil
}

// Run processes events until ctx is cancelled (returns nil) or a worker hits
// an error it can't recover from (returns that error). Either way it stops
// the workers, commits stored offsets, and releases all connections.
func (p *Processor) Run(ctx context.Context) (err error) {
	defer func() {
		p.close()
		// A worker can fail while shutdown is already under way. close()
		// waits for every worker, and a failing worker reports before it
		// exits, so any such error is in p.errs by now. Don't hide it behind
		// a clean exit.
		if err == nil {
			select {
			case err = <-p.errs:
			default:
			}
		}
	}()

	err = p.consumer.SubscribeTopics([]string{p.cfg.Kafka.Topic}, func(c *kafka.Consumer, ev kafka.Event) error {
		return p.rebalance(ctx, c, ev)
	})
	if err != nil {
		return fmt.Errorf("subscribe to %s: %w", p.cfg.Kafka.Topic, err)
	}

	log.Printf("Consuming from %s as group %s", p.cfg.Kafka.Topic, p.cfg.Kafka.GroupID)
	for {
		select {
		case <-ctx.Done():
			log.Print("Shutting down")
			return nil
		case err := <-p.errs:
			return err
		default:
		}

		switch e := p.consumer.Poll(100).(type) {
		case *kafka.Message:
			w := p.workers[e.TopicPartition.Partition]
			if w == nil {
				log.Printf("No worker for %s; skipping until reassigned", e.TopicPartition)
				continue
			}
			// Blocks when the partition's buffer is full: natural backpressure.
			select {
			case w.msgs <- e:
			case err := <-p.errs:
				return err
			case <-ctx.Done():
				log.Print("Shutting down")
				return nil
			}
		case kafka.Error:
			if e.IsFatal() {
				return fmt.Errorf("fatal Kafka error: %w", e)
			}
			log.Printf("Kafka error: %v", e)
		}
	}
}

func (p *Processor) rebalance(ctx context.Context, c *kafka.Consumer, ev kafka.Event) error {
	switch e := ev.(type) {
	case kafka.AssignedPartitions:
		log.Printf("Assigned partitions %v", partitionIDs(e.Partitions))
		for _, tp := range e.Partitions {
			p.startWorker(ctx, tp.Partition)
		}
		return c.Assign(e.Partitions)
	case kafka.RevokedPartitions:
		log.Printf("Revoking partitions %v", partitionIDs(e.Partitions))
		// Let in-flight events finish so their offsets can be committed
		// before another consumer takes over.
		for _, tp := range e.Partitions {
			p.stopWorker(tp.Partition)
		}
		if !c.AssignmentLost() {
			p.commit()
		}
		return c.Unassign()
	}
	return nil
}

func (p *Processor) startWorker(ctx context.Context, partition int32) {
	w := &worker{
		msgs: make(chan *kafka.Message, p.cfg.PartitionBuffer),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	p.workers[partition] = w

	go func() {
		defer close(w.done)
		for {
			// Check stop first so a full buffer doesn't delay shutdown.
			select {
			case <-w.stop:
				return
			default:
			}
			select {
			case <-w.stop:
				return
			case msg := <-w.msgs:
				if err := p.handle(ctx, msg); err != nil {
					if !errors.Is(err, errStopping) {
						select {
						case p.errs <- err:
						default: // another worker already reported
						}
					}
					return
				}
			}
		}
	}()
}

// stopWorker waits for the partition's in-flight event, if any. Messages
// still buffered are dropped; their offsets aren't stored, so they are
// fetched again by whoever owns the partition next.
func (p *Processor) stopWorker(partition int32) {
	w, ok := p.workers[partition]
	if !ok {
		return
	}
	close(w.stop)
	<-w.done
	delete(p.workers, partition)
}

func (p *Processor) commit() {
	if _, err := p.consumer.Commit(); err != nil {
		var kerr kafka.Error
		if errors.As(err, &kerr) && kerr.Code() == kafka.ErrNoOffset {
			return // nothing new to commit
		}
		log.Printf("Offset commit failed: %v", err)
	}
}

func (p *Processor) close() {
	for partition := range p.workers {
		p.stopWorker(partition)
	}
	// Unregister before closing the consumer so the final metric collection
	// doesn't query a closed consumer.
	if err := p.metrics.lagRegistration.Unregister(); err != nil {
		log.Printf("Failed to unregister lag callback: %v", err)
	}
	p.commit()
	if err := p.consumer.Close(); err != nil {
		log.Printf("Failed to close consumer: %v", err)
	}
	p.producer.Flush(5000)
	p.producer.Close()
	p.store.Close()
}

func partitionIDs(tps []kafka.TopicPartition) []int32 {
	ids := make([]int32, len(tps))
	for i, tp := range tps {
		ids[i] = tp.Partition
	}
	return ids
}
