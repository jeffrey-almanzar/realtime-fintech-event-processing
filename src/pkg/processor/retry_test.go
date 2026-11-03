package processor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/metric/noop"
)

func testProcessor(t *testing.T, maxAttempts int) *Processor {
	t.Helper()
	retries, err := noop.NewMeterProvider().Meter("test").Int64Counter("retries")
	if err != nil {
		t.Fatal(err)
	}
	return &Processor{
		cfg:     Config{Retry: RetryPolicy{MaxAttempts: maxAttempts, InitialBackoff: time.Millisecond, MaxBackoff: 4 * time.Millisecond}},
		metrics: &instruments{retries: retries},
	}
}

func TestIsPermanent(t *testing.T) {
	tests := map[string]struct {
		err  error
		want bool
	}{
		"rejection":            {&Rejection{Reason: "insufficient_funds"}, true},
		"wrapped rejection":    {fmt.Errorf("apply: %w", &Rejection{Reason: "x"}), true},
		"data exception":       {&pgconn.PgError{Code: "22003"}, true}, // numeric_value_out_of_range
		"constraint violation": {&pgconn.PgError{Code: "23514"}, true}, // check_violation
		"connection failure":   {&pgconn.PgError{Code: "08006"}, false},
		"serialization":        {&pgconn.PgError{Code: "40001"}, false},
		"plain error":          {errors.New("connection refused"), false},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := isPermanent(tt.err); got != tt.want {
				t.Errorf("isPermanent(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestRetrySucceedsAfterTransientErrors(t *testing.T) {
	p := testProcessor(t, 5)
	calls := 0
	err := p.retry(context.Background(), context.Background(), "op", func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Errorf("retry = %v after %d calls, want nil after 3", err, calls)
	}
}

func TestRetryGivesUpAfterMaxAttempts(t *testing.T) {
	p := testProcessor(t, 4)
	calls := 0
	err := p.retry(context.Background(), context.Background(), "op", func() error {
		calls++
		return errors.New("transient")
	})
	if err == nil || calls != 4 {
		t.Errorf("retry = %v after %d calls, want an error after 4", err, calls)
	}
}

func TestRetryStopsOnPermanentError(t *testing.T) {
	p := testProcessor(t, 5)
	calls := 0
	err := p.retry(context.Background(), context.Background(), "op", func() error {
		calls++
		return &Rejection{Reason: "insufficient_funds"}
	})
	var rej *Rejection
	if !errors.As(err, &rej) || calls != 1 {
		t.Errorf("retry = %v after %d calls, want the rejection after 1", err, calls)
	}
}

func TestRetryStopsOnShutdown(t *testing.T) {
	p := testProcessor(t, 100)
	p.cfg.Retry.InitialBackoff = time.Hour
	runCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.retry(context.Background(), runCtx, "op", func() error { return errors.New("transient") })
	if !errors.Is(err, errStopping) {
		t.Errorf("retry = %v, want errStopping", err)
	}
}
