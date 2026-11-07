package workflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/testsuite"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
)

// fakes records what the workflow did. Unlike activity mocks, these run the
// real activity code, including how errors are classified.
type fakes struct {
	mu        sync.Mutex
	chargeErr func(attempt int) error
	charges   int
	refundErr func(attempt int) error
	refunds   []*fintechv1.Transaction
	refundTry int
	outcomes  []string
	details   []string
}

func (f *fakes) Charge(context.Context, string, int64, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.charges++
	if f.chargeErr != nil {
		return f.chargeErr(f.charges)
	}
	return nil
}

func (f *fakes) Publish(_ context.Context, tx *fintechv1.Transaction) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refundTry++
	if f.refundErr != nil {
		if err := f.refundErr(f.refundTry); err != nil {
			return err
		}
	}
	f.refunds = append(f.refunds, tx)
	return nil
}

func (f *fakes) RecordOutcome(_ context.Context, _, status, detail string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.outcomes = append(f.outcomes, status)
	f.details = append(f.details, detail)
	return nil
}

var testInput = SettlementInput{EventID: uuid.NewString(), AccountID: "acct-1", AmountMinor: 2500, Currency: "USD"}

func run(t *testing.T, f *fakes) string {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(&Activities{Provider: f, Refunds: f, Outcomes: f})
	env.ExecuteWorkflow(SettleWithdrawal, testInput)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow failed: %v", err)
	}
	var status string
	if err := env.GetWorkflowResult(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func TestSuccessfulChargeSettlesWithoutRefund(t *testing.T) {
	f := &fakes{}
	if got := run(t, f); got != StatusSettled {
		t.Errorf("status = %q, want %q", got, StatusSettled)
	}
	if len(f.refunds) != 0 {
		t.Errorf("refunds = %d, want 0", len(f.refunds))
	}
	if len(f.outcomes) != 1 || f.outcomes[0] != StatusSettled {
		t.Errorf("outcomes = %v, want [settled]", f.outcomes)
	}
}

func TestDeclinedChargeIsRefundedWithoutRetrying(t *testing.T) {
	f := &fakes{chargeErr: func(int) error { return ErrDeclined }}
	if got := run(t, f); got != StatusReversed {
		t.Errorf("status = %q, want %q", got, StatusReversed)
	}
	if f.charges != 1 {
		t.Errorf("charges = %d, want 1 (a decline must not be retried)", f.charges)
	}
	if len(f.refunds) != 1 {
		t.Fatalf("refunds = %d, want 1", len(f.refunds))
	}
	r := f.refunds[0]
	if r.AccountId != testInput.AccountID || r.AmountMinor != testInput.AmountMinor || r.Currency != testInput.Currency ||
		r.Type != fintechv1.TransactionType_TRANSACTION_TYPE_REFUND {
		t.Errorf("refund = %v, doesn't match the withdrawal", r)
	}
	if r.EventId != RefundEventID(testInput.EventID) {
		t.Errorf("refund ID = %s, want the one derived from the withdrawal", r.EventId)
	}
	if len(f.outcomes) != 1 || f.outcomes[0] != StatusReversed {
		t.Errorf("outcomes = %v, want [reversed]", f.outcomes)
	}
	if f.details[0] != "payment declined by provider" {
		t.Errorf("detail = %q, want the provider's reason", f.details[0])
	}
}

func TestTransientFailuresAreRetriedUntilTheChargeSucceeds(t *testing.T) {
	f := &fakes{chargeErr: func(attempt int) error {
		if attempt <= 3 {
			return ErrUnavailable
		}
		return nil
	}}
	if got := run(t, f); got != StatusSettled {
		t.Errorf("status = %q, want %q", got, StatusSettled)
	}
	if f.charges != 4 {
		t.Errorf("charges = %d, want 4", f.charges)
	}
	if len(f.refunds) != 0 {
		t.Errorf("refunds = %d, want 0", len(f.refunds))
	}
}

func TestProviderDownPastTheDeadlineIsRefunded(t *testing.T) {
	f := &fakes{chargeErr: func(int) error { return ErrUnavailable }}
	if got := run(t, f); got != StatusReversed {
		t.Errorf("status = %q, want %q", got, StatusReversed)
	}
	if f.charges < 2 {
		t.Errorf("charges = %d, want several attempts before giving up", f.charges)
	}
	if len(f.refunds) != 1 {
		t.Errorf("refunds = %d, want 1", len(f.refunds))
	}
}

// The compensation can't be allowed to give up, or the customer loses money.
func TestRefundIsRetriedUntilItSucceeds(t *testing.T) {
	f := &fakes{
		chargeErr: func(int) error { return ErrDeclined },
		refundErr: func(attempt int) error {
			if attempt <= 5 {
				return errors.New("Kafka unavailable")
			}
			return nil
		},
	}
	if got := run(t, f); got != StatusReversed {
		t.Errorf("status = %q, want %q", got, StatusReversed)
	}
	if f.refundTry != 6 || len(f.refunds) != 1 {
		t.Errorf("refund tries = %d, delivered = %d; want 6 and 1", f.refundTry, len(f.refunds))
	}
}

func TestRefundEventIDIsDeterministicPerWithdrawal(t *testing.T) {
	a, b := uuid.NewString(), uuid.NewString()
	first, second := RefundEventID(a), RefundEventID(a)
	if first != second {
		t.Error("the same withdrawal produced different refund IDs")
	}
	if RefundEventID(a) == RefundEventID(b) {
		t.Error("different withdrawals produced the same refund ID")
	}
	if RefundEventID(a) == a {
		t.Error("the refund ID must differ from the withdrawal's")
	}
	if _, err := uuid.Parse(RefundEventID(a)); err != nil {
		t.Errorf("refund ID must pass event validation: %v", err)
	}
}

func TestMockProviderBehaviors(t *testing.T) {
	ctx := context.Background()
	p := &MockProvider{Delay: time.Nanosecond}

	if err := p.Charge(ctx, "a", 1013, "USD"); !errors.Is(err, ErrDeclined) {
		t.Errorf("..13: %v, want ErrDeclined", err)
	}
	if err := p.Charge(ctx, "b", 1098, "USD"); !errors.Is(err, ErrUnavailable) {
		t.Errorf("..98: %v, want ErrUnavailable", err)
	}
	for i, want := range []error{ErrUnavailable, ErrUnavailable, nil} {
		if err := p.Charge(ctx, "c", 1099, "USD"); !errors.Is(err, want) {
			t.Errorf("..99 attempt %d: %v, want %v", i+1, err, want)
		}
	}
	if err := p.Charge(ctx, "d", 2500, "USD"); err != nil {
		t.Errorf("other amounts: %v, want success", err)
	}
}
