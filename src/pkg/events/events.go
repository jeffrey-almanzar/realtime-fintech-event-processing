// Package events holds the validation rules for Transaction events. Both the
// Event API (at the edge) and the Processor (defensively) apply them.
package events

import (
	"errors"
	"fmt"

	"github.com/google/uuid"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
)

// SupportedCurrencies lists the ISO 4217 codes the platform accepts.
var SupportedCurrencies = map[string]bool{
	"USD": true, "EUR": true, "GBP": true, "CAD": true, "MXN": true, "BRL": true, "JPY": true,
}

// Validate reports why tx can never be applied, or nil if it is well formed.
// It does not check business rules that depend on account state.
func Validate(tx *fintechv1.Transaction) error {
	if _, err := uuid.Parse(tx.EventId); err != nil {
		return errors.New("event_id must be a UUID")
	}
	if tx.AccountId == "" {
		return errors.New("account_id is required")
	}
	if _, err := SignedAmount(tx); err != nil {
		return err
	}
	if tx.AmountMinor <= 0 {
		return errors.New("amount_minor must be a positive integer")
	}
	if !SupportedCurrencies[tx.Currency] {
		return fmt.Errorf("unsupported currency %q", tx.Currency)
	}
	if tx.CreatedAt == nil {
		return errors.New("created_at is required")
	}
	return nil
}

// SignedAmount returns how much tx changes the account balance.
func SignedAmount(tx *fintechv1.Transaction) (int64, error) {
	switch tx.Type {
	case fintechv1.TransactionType_TRANSACTION_TYPE_DEPOSIT,
		fintechv1.TransactionType_TRANSACTION_TYPE_TRANSFER_IN,
		fintechv1.TransactionType_TRANSACTION_TYPE_REFUND:
		return tx.AmountMinor, nil
	case fintechv1.TransactionType_TRANSACTION_TYPE_WITHDRAWAL,
		fintechv1.TransactionType_TRANSACTION_TYPE_TRANSFER_OUT:
		return -tx.AmountMinor, nil
	default:
		return 0, fmt.Errorf("unsupported transaction type %s", tx.Type)
	}
}
