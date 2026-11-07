package workflow

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrDeclined means the provider refused the charge; retrying can't help.
var ErrDeclined = errors.New("declined")

// ErrUnavailable means the provider couldn't be reached; retrying may help.
var ErrUnavailable = errors.New("provider unavailable")

// Provider charges money to an external payment network.
type Provider interface {
	// Charge is idempotent per key: repeating a key never charges twice.
	Charge(ctx context.Context, key string, amountMinor int64, currency string) error
}

// MockProvider stands in for a real payment provider. The last two digits of
// the amount choose the behavior, so a demo can trigger each path on purpose:
//
//	..13  declined
//	..98  unavailable until the workflow gives up
//	..99  unavailable twice, then succeeds
//	else  succeeds
type MockProvider struct {
	Delay time.Duration

	mu       sync.Mutex
	attempts map[string]int
}

// Charge implements Provider.
func (m *MockProvider) Charge(ctx context.Context, key string, amountMinor int64, _ string) error {
	select {
	case <-time.After(m.Delay):
	case <-ctx.Done():
		return ctx.Err()
	}

	switch amountMinor % 100 {
	case 13:
		return ErrDeclined
	case 98:
		return ErrUnavailable
	case 99:
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.attempts == nil {
			m.attempts = make(map[string]int)
		}
		m.attempts[key]++
		if m.attempts[key] <= 2 {
			return ErrUnavailable
		}
	}
	return nil
}
