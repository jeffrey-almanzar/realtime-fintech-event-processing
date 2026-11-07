package query

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Channel is the Postgres NOTIFY channel written by the accounts trigger
// (see migrations/000002_account_updates.up.sql).
const Channel = "account_updates"

// Listener holds a dedicated Postgres connection that LISTENs for account
// changes and publishes them to a Hub.
type Listener struct {
	url   string
	hub   *Hub
	store *Store

	notifications metric.Int64Counter
	pushLatency   metric.Float64Histogram
}

func NewListener(url string, hub *Hub, store *Store) (*Listener, error) {
	meter := otel.Meter(instrumentationName)
	notifications, err := meter.Int64Counter("account.updates.received",
		metric.WithDescription("Account change notifications received from Postgres"),
		metric.WithUnit("{update}"))
	if err != nil {
		return nil, err
	}
	pushLatency, err := meter.Float64Histogram("transaction.push.latency",
		metric.WithDescription("Time from event creation (created_at) until the balance change reaches the push layer"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 30000, 60000))
	if err != nil {
		return nil, err
	}
	return &Listener{url: url, hub: hub, store: store, notifications: notifications, pushLatency: pushLatency}, nil
}

// Run listens until ctx is cancelled, reconnecting with backoff when the
// connection drops.
func (l *Listener) Run(ctx context.Context) {
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		err := l.listen(ctx, func() { backoff = 100 * time.Millisecond })
		if ctx.Err() != nil {
			return
		}
		log.Printf("LISTEN connection lost, reconnecting in %s: %v", backoff, err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		backoff = min(2*backoff, 10*time.Second)
	}
}

func (l *Listener) listen(ctx context.Context, connected func()) error {
	conn, err := pgx.Connect(ctx, l.url)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	if _, err := conn.Exec(ctx, "LISTEN "+Channel); err != nil {
		return err
	}
	connected()
	log.Printf("Listening for changes on %s", Channel)

	// Changes made while disconnected were never delivered; send every
	// watched account a fresh snapshot so no client stays stale.
	l.resync(ctx)

	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		var a Account
		if err := json.Unmarshal([]byte(n.Payload), &a); err != nil {
			log.Printf("Ignoring malformed notification: %v", err)
			continue
		}
		l.notifications.Add(ctx, 1)
		if a.LastEventAt != nil {
			l.pushLatency.Record(ctx, float64(time.Since(*a.LastEventAt))/float64(time.Millisecond))
		}
		l.hub.Publish(a)
	}
}

func (l *Listener) resync(ctx context.Context) {
	for _, id := range l.hub.Accounts() {
		a, err := l.store.Account(ctx, id)
		if err != nil {
			continue
		}
		l.hub.Publish(a)
	}
}
