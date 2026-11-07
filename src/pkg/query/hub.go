package query

import "sync"

// Hub fans account updates out to the clients watching each account.
type Hub struct {
	mu   sync.Mutex
	subs map[string]map[*Subscription]struct{}
}

func NewHub() *Hub {
	return &Hub{subs: make(map[string]map[*Subscription]struct{})}
}

// Subscription receives updates for one account. Only the latest pending
// update is kept: a slow client skips intermediate balances instead of
// slowing everyone else down or falling further and further behind.
type Subscription struct {
	C       <-chan Account
	c       chan Account
	hub     *Hub
	account string
}

func (h *Hub) Subscribe(accountID string) *Subscription {
	c := make(chan Account, 1)
	s := &Subscription{C: c, c: c, hub: h, account: accountID}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs[accountID] == nil {
		h.subs[accountID] = make(map[*Subscription]struct{})
	}
	h.subs[accountID][s] = struct{}{}
	return s
}

func (s *Subscription) Close() {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()
	delete(s.hub.subs[s.account], s)
	if len(s.hub.subs[s.account]) == 0 {
		delete(s.hub.subs, s.account)
	}
}

// Publish delivers a to every subscriber of its account without blocking.
func (h *Hub) Publish(a Account) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.subs[a.AccountID] {
		select {
		case s.c <- a:
		default:
			// Replace the stale pending update with the newer one.
			select {
			case <-s.c:
			default:
			}
			s.c <- a
		}
	}
}

// Accounts lists the accounts that have at least one subscriber.
func (h *Hub) Accounts() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	ids := make([]string, 0, len(h.subs))
	for id := range h.subs {
		ids = append(ids, id)
	}
	return ids
}

// Subscribers counts open subscriptions across all accounts.
func (h *Hub) Subscribers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, s := range h.subs {
		n += len(s)
	}
	return n
}
