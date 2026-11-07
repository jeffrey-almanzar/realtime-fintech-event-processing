package workflow

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

const (
	relayInterval  = 200 * time.Millisecond
	relayBatchSize = 200
)

// Starter starts a settlement workflow. Starting one that already exists must
// succeed, so a relay that crashed before recording its progress can repeat.
type Starter interface {
	Start(ctx context.Context, in SettlementInput) error
}

// Relay moves settlements from the outbox table to Temporal. It runs outside
// the Processor, so a slow or unavailable Temporal only delays settlements:
// rows wait in the table and events keep flowing.
type Relay struct {
	Pool    *pgxpool.Pool
	Starter Starter
}

// Run relays until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(relayInterval)
	defer ticker.Stop()
	for {
		// Keep going while there's a backlog, then wait for the next tick.
		n, err := r.RelayOnce(ctx)
		if err != nil && ctx.Err() == nil {
			log.Printf("Settlement relay error: %v", err)
		}
		if err == nil && n == relayBatchSize {
			continue
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

const selectPending = `
SELECT event_id, account_id, amount_minor, currency FROM settlements
WHERE status = 'pending'
ORDER BY created_at
LIMIT $1
FOR UPDATE SKIP LOCKED`

// RelayOnce starts one batch of workflows and returns how many it started.
// The rows stay locked until the status change commits, so several relays can
// run without doing the same work, and a workflow's outcome can't be recorded
// before its row is marked started.
func (r *Relay) RelayOnce(ctx context.Context) (int, error) {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, selectPending, relayBatchSize)
	if err != nil {
		return 0, err
	}
	pending, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (SettlementInput, error) {
		var in SettlementInput
		err := row.Scan(&in.EventID, &in.AccountID, &in.AmountMinor, &in.Currency)
		in.Currency = strings.TrimSpace(in.Currency) // CHAR(3) pads
		return in, err
	})
	if err != nil {
		return 0, err
	}

	// Record what started even if a later one fails; the rest stay pending.
	var started []string
	var startErr error
	for _, in := range pending {
		if startErr = r.Starter.Start(ctx, in); startErr != nil {
			break
		}
		started = append(started, in.EventID)
	}
	if len(started) > 0 {
		_, err := tx.Exec(ctx, "UPDATE settlements SET status = 'started', updated_at = now() WHERE event_id = ANY($1)", started)
		if err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(started), startErr
}

// TemporalStarter starts settlements on a Temporal server.
type TemporalStarter struct {
	Client client.Client
}

// Start implements Starter. The workflow ID comes from the event, so one
// withdrawal can only ever have one settlement.
func (s *TemporalStarter) Start(ctx context.Context, in SettlementInput) error {
	_, err := s.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                                       "settlement-" + in.EventID,
		TaskQueue:                                TaskQueue,
		WorkflowIDReusePolicy:                    enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
		RetryPolicy:                              &temporal.RetryPolicy{MaximumAttempts: 1},
	}, SettleWithdrawal, in)
	var already *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &already) {
		return nil
	}
	return err
}
