package query

import (
	"testing"
	"time"
)

func receive(t *testing.T, s *Subscription) Account {
	t.Helper()
	select {
	case a := <-s.C:
		return a
	case <-time.After(time.Second):
		t.Fatal("no update received")
		return Account{}
	}
}

func TestHubDeliversOnlyToSubscribersOfTheAccount(t *testing.T) {
	h := NewHub()
	a1, a2 := h.Subscribe("a"), h.Subscribe("a")
	b := h.Subscribe("b")

	h.Publish(Account{AccountID: "a", Version: 1})

	if got := receive(t, a1); got.Version != 1 {
		t.Errorf("first subscriber got version %d, want 1", got.Version)
	}
	if got := receive(t, a2); got.Version != 1 {
		t.Errorf("second subscriber got version %d, want 1", got.Version)
	}
	select {
	case got := <-b.C:
		t.Errorf("subscriber of b received %v", got)
	default:
	}
}

func TestHubKeepsOnlyLatestForSlowSubscriber(t *testing.T) {
	h := NewHub()
	s := h.Subscribe("a")
	for v := int64(1); v <= 5; v++ {
		h.Publish(Account{AccountID: "a", Version: v}) // never blocks
	}
	if got := receive(t, s); got.Version != 5 {
		t.Errorf("got version %d, want the latest (5)", got.Version)
	}
}

func TestHubClose(t *testing.T) {
	h := NewHub()
	s := h.Subscribe("a")
	other := h.Subscribe("a")
	if h.Subscribers() != 2 {
		t.Fatalf("Subscribers() = %d, want 2", h.Subscribers())
	}

	s.Close()
	other.Close()
	if h.Subscribers() != 0 || len(h.Accounts()) != 0 {
		t.Errorf("after Close: %d subscribers, accounts %v; want none", h.Subscribers(), h.Accounts())
	}
	h.Publish(Account{AccountID: "a", Version: 1}) // must not panic or block
}
