package events

import (
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
)

func validTx() *fintechv1.Transaction {
	return &fintechv1.Transaction{
		EventId:     "0f780f4d-69be-4e17-b013-62d43342def1",
		AccountId:   "acct-1",
		Type:        fintechv1.TransactionType_TRANSACTION_TYPE_DEPOSIT,
		AmountMinor: 100,
		Currency:    "USD",
		CreatedAt:   timestamppb.Now(),
	}
}

func TestValidateAcceptsValidEvent(t *testing.T) {
	if err := Validate(validTx()); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestValidateRejects(t *testing.T) {
	tests := map[string]func(*fintechv1.Transaction){
		"non-UUID event_id":    func(tx *fintechv1.Transaction) { tx.EventId = "abc" },
		"missing account":      func(tx *fintechv1.Transaction) { tx.AccountId = "" },
		"unspecified type":     func(tx *fintechv1.Transaction) { tx.Type = fintechv1.TransactionType_TRANSACTION_TYPE_UNSPECIFIED },
		"zero amount":          func(tx *fintechv1.Transaction) { tx.AmountMinor = 0 },
		"negative amount":      func(tx *fintechv1.Transaction) { tx.AmountMinor = -1 },
		"unsupported currency": func(tx *fintechv1.Transaction) { tx.Currency = "XYZ" },
		"lowercase currency":   func(tx *fintechv1.Transaction) { tx.Currency = "usd" },
		"missing created_at":   func(tx *fintechv1.Transaction) { tx.CreatedAt = nil },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			tx := validTx()
			mutate(tx)
			if err := Validate(tx); err == nil {
				t.Error("Validate() = nil, want an error")
			}
		})
	}
}

func TestSignedAmount(t *testing.T) {
	tests := []struct {
		txType fintechv1.TransactionType
		want   int64
	}{
		{fintechv1.TransactionType_TRANSACTION_TYPE_DEPOSIT, 250},
		{fintechv1.TransactionType_TRANSACTION_TYPE_TRANSFER_IN, 250},
		{fintechv1.TransactionType_TRANSACTION_TYPE_WITHDRAWAL, -250},
		{fintechv1.TransactionType_TRANSACTION_TYPE_TRANSFER_OUT, -250},
	}
	for _, tt := range tests {
		t.Run(tt.txType.String(), func(t *testing.T) {
			got, err := SignedAmount(&fintechv1.Transaction{Type: tt.txType, AmountMinor: 250})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("SignedAmount = %d, want %d", got, tt.want)
			}
		})
	}
}
