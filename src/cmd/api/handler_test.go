package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
)

type fakePublisher struct {
	published []*fintechv1.Transaction
	err       error
}

func (f *fakePublisher) Publish(_ context.Context, tx *fintechv1.Transaction) error {
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, tx)
	return nil
}

func post(t *testing.T, pub Publisher, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/transactions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handleCreateTransaction(pub).ServeHTTP(rec, req)
	return rec
}

func TestCreateTransactionAccepted(t *testing.T) {
	pub := &fakePublisher{}
	rec := post(t, pub, `{"account_id":"acct-1","type":"deposit","amount_minor":1500,"currency":"USD"}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusAccepted, rec.Body)
	}
	if len(pub.published) != 1 {
		t.Fatalf("published %d events, want 1", len(pub.published))
	}
	tx := pub.published[0]
	if tx.AccountId != "acct-1" || tx.AmountMinor != 1500 || tx.Type != fintechv1.TransactionType_TRANSACTION_TYPE_DEPOSIT {
		t.Errorf("unexpected event: %v", tx)
	}
	if tx.EventId == "" || tx.CreatedAt == nil {
		t.Errorf("event_id and created_at must be set: %v", tx)
	}

	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp["event_id"] != tx.EventId {
		t.Errorf("response = %s, want event_id %s", rec.Body, tx.EventId)
	}
}

func TestCreateTransactionKeepsClientEventID(t *testing.T) {
	pub := &fakePublisher{}
	id := "0f780f4d-69be-4e17-b013-62d43342def1"
	rec := post(t, pub, `{"event_id":"`+id+`","account_id":"a","type":"withdrawal","amount_minor":1,"currency":"EUR"}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}
	if got := pub.published[0].EventId; got != id {
		t.Errorf("EventId = %q, want %q", got, id)
	}
}

func TestCreateTransactionValidation(t *testing.T) {
	tests := map[string]string{
		"malformed JSON":   `{`,
		"unknown field":    `{"account_id":"a","type":"deposit","amount_minor":1,"currency":"USD","extra":1}`,
		"missing account":  `{"type":"deposit","amount_minor":1,"currency":"USD"}`,
		"bad type":         `{"account_id":"a","type":"refund","amount_minor":1,"currency":"USD"}`,
		"zero amount":      `{"account_id":"a","type":"deposit","amount_minor":0,"currency":"USD"}`,
		"negative amount":  `{"account_id":"a","type":"deposit","amount_minor":-5,"currency":"USD"}`,
		"float amount":     `{"account_id":"a","type":"deposit","amount_minor":1.5,"currency":"USD"}`,
		"bad currency":     `{"account_id":"a","type":"deposit","amount_minor":1,"currency":"usd"}`,
		"non-UUID eventID": `{"event_id":"abc","account_id":"a","type":"deposit","amount_minor":1,"currency":"USD"}`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			pub := &fakePublisher{}
			rec := post(t, pub, body)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d (body: %s)", rec.Code, http.StatusBadRequest, rec.Body)
			}
			if len(pub.published) != 0 {
				t.Errorf("published %d events, want 0", len(pub.published))
			}
		})
	}
}

func TestCreateTransactionPublishFailure(t *testing.T) {
	pub := &fakePublisher{err: errors.New("broker down")}
	rec := post(t, pub, `{"account_id":"a","type":"deposit","amount_minor":1,"currency":"USD"}`)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}
