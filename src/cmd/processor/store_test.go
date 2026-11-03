package main

import (
	"testing"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
)

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
			got, err := signedAmount(&fintechv1.Transaction{Type: tt.txType, AmountMinor: 250})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("signedAmount = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestSignedAmountRejectsUnspecified(t *testing.T) {
	if _, err := signedAmount(&fintechv1.Transaction{AmountMinor: 250}); err == nil {
		t.Error("expected an error for TRANSACTION_TYPE_UNSPECIFIED")
	}
}
