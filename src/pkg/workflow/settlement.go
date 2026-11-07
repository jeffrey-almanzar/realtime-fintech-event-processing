// Package workflow settles withdrawals with Temporal. By the time a settlement
// starts, the account has already been debited, so the workflow only has two
// ways to finish: the payment provider takes the money, or the account gets it
// back. Temporal keeps the progress durable, so a worker crash at any step
// resumes from that step.
package workflow

import (
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// TaskQueue is where settlement workflows and activities run.
const TaskQueue = "settlements"

// Outcomes recorded on the settlement.
const (
	StatusSettled  = "settled"
	StatusReversed = "reversed"
)

// declinedErrorType marks a provider refusal, which no retry can change.
const declinedErrorType = "PaymentDeclined"

// chargeDeadline is how long an unavailable provider is retried before the
// withdrawal is given up on and refunded.
const chargeDeadline = 2 * time.Minute

// SettlementInput identifies the withdrawal being settled.
type SettlementInput struct {
	EventID     string
	AccountID   string
	AmountMinor int64
	Currency    string
}

// SettleWithdrawal charges the payment provider and, if that fails for good,
// compensates by refunding the account.
func SettleWithdrawal(ctx workflow.Context, in SettlementInput) (string, error) {
	var a *Activities

	chargeCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:    30 * time.Second,
		ScheduleToCloseTimeout: chargeDeadline,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2,
			MaximumInterval:        20 * time.Second,
			NonRetryableErrorTypes: []string{declinedErrorType},
		},
	})
	// The refund money and the record of the outcome must not be lost, so
	// these retry until they succeed.
	mustFinishCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	})

	chargeErr := workflow.ExecuteActivity(chargeCtx, a.ChargeProvider, in).Get(ctx, nil)
	if chargeErr == nil {
		err := workflow.ExecuteActivity(mustFinishCtx, a.RecordOutcome, in.EventID, StatusSettled, "").Get(ctx, nil)
		return StatusSettled, err
	}

	workflow.GetLogger(ctx).Info("Charge failed, refunding", "event_id", in.EventID, "error", chargeErr)
	if err := workflow.ExecuteActivity(mustFinishCtx, a.PublishRefund, in).Get(ctx, nil); err != nil {
		return "", err
	}
	err := workflow.ExecuteActivity(mustFinishCtx, a.RecordOutcome, in.EventID, StatusReversed, failureDetail(chargeErr)).Get(ctx, nil)
	return StatusReversed, err
}

// failureDetail returns the provider's reason rather than Temporal's wrapping.
func failureDetail(err error) string {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		return appErr.Message()
	}
	return err.Error()
}
