// Processor: consumes financial events from Kafka and applies them to account
// balances in Postgres. See src/pkg/processor for how ordering, retries, the
// DLQ, and offset commits work.

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/processor"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/telemetry"
)

func main() {
	if err := run(); err != nil {
		log.Printf("Processor stopped: %v", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTelemetry, err := telemetry.Init(ctx, "transaction-processor")
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

	telemetry.StartPprof()

	p, err := processor.New(ctx, processor.DefaultConfig())
	if err != nil {
		return err
	}
	return p.Run(ctx)
}
