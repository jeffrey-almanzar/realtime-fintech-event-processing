// Settlement worker: runs the Temporal workflows that settle withdrawals, plus
// the relay that starts them from the settlements outbox table.

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/config"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/telemetry"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/workflow"
)

func main() {
	if err := run(); err != nil {
		log.Printf("Worker stopped: %v", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTelemetry, err := telemetry.Init(ctx, "settlement-worker")
	if err != nil {
		return err
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTelemetry(flushCtx); err != nil {
			log.Printf("Telemetry shutdown error: %v", err)
		}
	}()

	pool, err := pgxpool.New(ctx, config.DatabaseURL())
	if err != nil {
		return err
	}
	defer pool.Close()

	cfg := config.LoadKafka()
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.BootstrapServers,
		"enable.idempotence": true,
		"acks":               "all",
	})
	if err != nil {
		return err
	}
	defer producer.Close()

	temporal, err := client.Dial(client.Options{HostPort: config.TemporalAddress()})
	if err != nil {
		return err
	}
	defer temporal.Close()

	outcomes, err := workflow.NewPostgresOutcomes(pool)
	if err != nil {
		return err
	}

	w := worker.New(temporal, workflow.TaskQueue, worker.Options{})
	w.RegisterWorkflow(workflow.SettleWithdrawal)
	w.RegisterActivity(&workflow.Activities{
		Provider: &workflow.MockProvider{Delay: config.ProviderDelay()},
		Refunds:  &workflow.KafkaPublisher{Producer: producer, Topic: cfg.Topic},
		Outcomes: outcomes,
	})

	relay := &workflow.Relay{Pool: pool, Starter: &workflow.TemporalStarter{Client: temporal}}
	go relay.Run(ctx)

	log.Printf("Settling withdrawals on task queue %s", workflow.TaskQueue)
	return w.Run(stopOn(ctx))
}

// stopOn adapts ctx to the channel worker.Run waits on.
func stopOn(ctx context.Context) <-chan any {
	c := make(chan any)
	go func() {
		<-ctx.Done()
		close(c)
	}()
	return c
}
