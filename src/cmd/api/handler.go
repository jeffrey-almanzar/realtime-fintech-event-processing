package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
)

// Publisher sends a validated transaction to the event log.
type Publisher interface {
	Publish(ctx context.Context, tx *fintechv1.Transaction) error
}

type transactionRequest struct {
	// Optional. Clients that retry should send the same ID so the processor
	// can deduplicate; one is generated when omitted.
	EventID     string `json:"event_id"`
	AccountID   string `json:"account_id"`
	Type        string `json:"type"`
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}

var transactionTypes = map[string]fintechv1.TransactionType{
	"deposit":      fintechv1.TransactionType_TRANSACTION_TYPE_DEPOSIT,
	"withdrawal":   fintechv1.TransactionType_TRANSACTION_TYPE_WITHDRAWAL,
	"transfer_in":  fintechv1.TransactionType_TRANSACTION_TYPE_TRANSFER_IN,
	"transfer_out": fintechv1.TransactionType_TRANSACTION_TYPE_TRANSFER_OUT,
}

var currencyCode = regexp.MustCompile(`^[A-Z]{3}$`)

// toTransaction validates the request and converts it to the event schema.
func (r transactionRequest) toTransaction() (*fintechv1.Transaction, error) {
	if r.AccountID == "" {
		return nil, errors.New("account_id is required")
	}
	txType, ok := transactionTypes[r.Type]
	if !ok {
		return nil, fmt.Errorf("type must be one of deposit, withdrawal, transfer_in, transfer_out")
	}
	if r.AmountMinor <= 0 {
		return nil, errors.New("amount_minor must be a positive integer")
	}
	if !currencyCode.MatchString(r.Currency) {
		return nil, errors.New("currency must be a 3-letter ISO 4217 code, e.g. USD")
	}

	eventID := r.EventID
	if eventID == "" {
		eventID = uuid.NewString()
	} else if _, err := uuid.Parse(eventID); err != nil {
		return nil, errors.New("event_id must be a UUID")
	}

	return &fintechv1.Transaction{
		EventId:     eventID,
		AccountId:   r.AccountID,
		Type:        txType,
		AmountMinor: r.AmountMinor,
		Currency:    r.Currency,
		CreatedAt:   timestamppb.Now(),
	}, nil
}

// handleCreateTransaction accepts a transaction and responds once Kafka has
// durably stored it. 202 means "accepted for processing", not "applied".
func handleCreateTransaction(pub Publisher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req transactionRequest
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body: " + err.Error()})
			return
		}

		tx, err := req.toTransaction()
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		if err := pub.Publish(r.Context(), tx); err != nil {
			log.Printf("Failed to publish event %s: %v", tx.EventId, err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "failed to publish event"})
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"event_id": tx.EventId})
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("Failed to write response: %v", err)
	}
}
