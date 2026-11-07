// Package query serves account state to users: a read API over Postgres and
// a live stream of balance changes, fed by Postgres LISTEN/NOTIFY.
package query

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Account is the current state of one account. It is also the payload of
// every live update.
type Account struct {
	AccountID    string     `json:"account_id"`
	Currency     string     `json:"currency"`
	BalanceMinor int64      `json:"balance_minor"`
	Version      int64      `json:"version"`
	UpdatedAt    time.Time  `json:"updated_at"`
	LastEventID  *string    `json:"last_event_id"`
	LastEventAt  *time.Time `json:"last_event_at"`
}

// Transaction is one applied event in an account's history.
type Transaction struct {
	EventID     string    `json:"event_id"`
	Type        string    `json:"type"`
	AmountMinor int64     `json:"amount_minor"` // signed: negative for debits
	Currency    string    `json:"currency"`
	CreatedAt   time.Time `json:"created_at"`
	ProcessedAt time.Time `json:"processed_at"`
}

// ErrNotFound means the account has never had an applied event.
var ErrNotFound = errors.New("account not found")

// Store reads account state from Postgres.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.Tracer = otelpgx.NewTracer()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

const selectAccount = `
SELECT account_id, currency, balance_minor, version, updated_at, last_event_id, last_event_at
FROM accounts WHERE account_id = $1`

func (s *Store) Account(ctx context.Context, id string) (Account, error) {
	var a Account
	err := s.pool.QueryRow(ctx, selectAccount, id).Scan(
		&a.AccountID, &a.Currency, &a.BalanceMinor, &a.Version, &a.UpdatedAt, &a.LastEventID, &a.LastEventAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, ErrNotFound
	}
	a.Currency = strings.TrimSpace(a.Currency) // CHAR(3) pads
	return a, err
}

// Newest first. Keyset pagination on (created_at, event_id) stays fast however
// deep the client pages, unlike OFFSET.
const selectTransactions = `
SELECT event_id, type, amount_minor, currency, created_at, processed_at
FROM processed_events
WHERE account_id = $1
  AND ($2::timestamptz IS NULL OR (created_at, event_id) < ($2, $3))
ORDER BY created_at DESC, event_id DESC
LIMIT $4`

// Transactions returns up to limit applied events older than cursor (or the
// newest when cursor is empty), plus the cursor for the next page ("" when
// there are no more).
func (s *Store) Transactions(ctx context.Context, accountID, cursor string, limit int) ([]Transaction, string, error) {
	var before *time.Time
	var beforeID string
	if cursor != "" {
		t, id, err := decodeCursor(cursor)
		if err != nil {
			return nil, "", err
		}
		before, beforeID = &t, id
	}

	// Fetch one extra row to learn whether another page exists.
	rows, err := s.pool.Query(ctx, selectTransactions, accountID, before, beforeID, limit+1)
	if err != nil {
		return nil, "", err
	}
	txs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Transaction, error) {
		var t Transaction
		err := row.Scan(&t.EventID, &t.Type, &t.AmountMinor, &t.Currency, &t.CreatedAt, &t.ProcessedAt)
		t.Currency = strings.TrimSpace(t.Currency)
		t.Type, t.AmountMinor = presentType(t.Type, t.AmountMinor)
		return t, err
	})
	if err != nil {
		return nil, "", err
	}

	next := ""
	if len(txs) > limit {
		txs = txs[:limit]
		last := txs[len(txs)-1]
		next = encodeCursor(last.CreatedAt, last.EventID)
	}
	return txs, next, nil
}

// presentType turns the stored enum name (TRANSACTION_TYPE_WITHDRAWAL) into the
// API's name (withdrawal) and signs the amount.
func presentType(stored string, amount int64) (string, int64) {
	name := strings.ToLower(strings.TrimPrefix(stored, "TRANSACTION_TYPE_"))
	if name == "withdrawal" || name == "transfer_out" {
		amount = -amount
	}
	return name, amount
}

// ErrBadCursor means the client sent a cursor this server didn't issue.
var ErrBadCursor = errors.New("invalid cursor")

func encodeCursor(t time.Time, eventID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(t.Format(time.RFC3339Nano) + "|" + eventID))
}

func decodeCursor(c string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return time.Time{}, "", ErrBadCursor
	}
	ts, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return time.Time{}, "", ErrBadCursor
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("%w: %v", ErrBadCursor, err)
	}
	return t, id, nil
}
