package query

import (
	"errors"
	"testing"
	"time"
)

func TestCursorRoundTrip(t *testing.T) {
	ts := time.Date(2026, 11, 3, 12, 30, 0, 123456000, time.UTC)
	gotTime, gotID, err := decodeCursor(encodeCursor(ts, "evt-1"))
	if err != nil {
		t.Fatal(err)
	}
	if !gotTime.Equal(ts) || gotID != "evt-1" {
		t.Errorf("decoded %v/%q, want %v/%q", gotTime, gotID, ts, "evt-1")
	}
}

func TestDecodeCursorRejectsGarbage(t *testing.T) {
	for _, c := range []string{"!!!", "bm8tc2VwYXJhdG9y", "bm90LWEtdGltZXxpZA"} {
		if _, _, err := decodeCursor(c); !errors.Is(err, ErrBadCursor) {
			t.Errorf("decodeCursor(%q) = %v, want ErrBadCursor", c, err)
		}
	}
}

func TestPresentType(t *testing.T) {
	tests := []struct {
		stored     string
		wantName   string
		wantAmount int64
	}{
		{"TRANSACTION_TYPE_DEPOSIT", "deposit", 100},
		{"TRANSACTION_TYPE_TRANSFER_IN", "transfer_in", 100},
		{"TRANSACTION_TYPE_WITHDRAWAL", "withdrawal", -100},
		{"TRANSACTION_TYPE_TRANSFER_OUT", "transfer_out", -100},
	}
	for _, tt := range tests {
		name, amount := presentType(tt.stored, 100)
		if name != tt.wantName || amount != tt.wantAmount {
			t.Errorf("presentType(%s) = %s/%d, want %s/%d", tt.stored, name, amount, tt.wantName, tt.wantAmount)
		}
	}
}
