// Defines a Temporal workflow to ensure data consistency.
// Wired up in Phase 6; kept compiling until then.

package workflow

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func StartTemporalWorker() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalf("Unable to create Temporal client: %v", err)
	}
	defer c.Close()

	w := worker.New(c, "financial-task-queue", worker.Options{})

	w.RegisterWorkflow(SampleWorkflow)
	w.RegisterActivity(SampleActivity)

	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalf("Unable to start worker: %v", err)
	}
}

func SampleWorkflow(ctx workflow.Context, value string) error {
	// Workflow logic
	return nil
}

func SampleActivity(ctx context.Context, value string) error {
	// Activity logic
	return nil
}
