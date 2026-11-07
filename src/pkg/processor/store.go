package processor

import (
	"context"
	"errors"
	"fmt"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
)

// Rejection is a business-rule failure: the event is well formed but can't be
// applied given the account's current state. Rejected events go to the DLQ.
type Rejection struct {
	Reason string // machine-readable, e.g. "insufficient_funds"
	Detail string
}

func (r *Rejection) Error() string { return r.Reason + ": " + r.Detail }

// Store applies transactions to account balances in Postgres.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore connects to Postgres. maxConns should be at least the number of
// partitions processed concurrently.
func NewStore(ctx context.Context, url string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.Tracer = otelpgx.NewTracer()
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}

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

const insertEvent = `
INSERT INTO processed_events
    (event_id, account_id, type, amount_minor, currency, created_at, kafka_partition, kafka_offset)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (event_id) DO NOTHING`

const lockAccount = `
SELECT balance_minor, currency FROM accounts WHERE account_id = $1 FOR UPDATE`

// last_event_* let readers tie a balance change to the event that caused it
// (and measure latency up to the push to clients).
const updateBalance = `
UPDATE accounts
SET balance_minor = $2, updated_at = now(), last_event_id = $3, last_event_at = $4
WHERE account_id = $1`

const createAccount = `
INSERT INTO accounts (account_id, currency, balance_minor, last_event_id, last_event_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (account_id) DO NOTHING`

// Apply records the event and updates the balance in one database
// transaction. It reports false when the event was already applied (a
// redelivery), and returns a *Rejection when a business rule fails; in both
// cases the balance is left untouched.
func (s *Store) Apply(ctx context.Context, tx *fintechv1.Transaction, delta int64, partition int32, offset int64) (bool, error) {
	dbtx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = dbtx.Rollback(ctx) }() // no-op after Commit

	tag, err := dbtx.Exec(ctx, insertEvent,
		tx.EventId, tx.AccountId, tx.Type.String(), tx.AmountMinor, tx.Currency,
		tx.CreatedAt.AsTime(), partition, offset)
	if err != nil {
		return false, fmt.Errorf("record event: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, dbtx.Commit(ctx)
	}

	// The row lock protects the balance even if two processors briefly own
	// the same partition during a rebalance.
	var balance int64
	var currency string
	err = dbtx.QueryRow(ctx, lockAccount, tx.AccountId).Scan(&balance, &currency)
	exists := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("lock account: %w", err)
	}

	if exists && currency != tx.Currency {
		return false, &Rejection{Reason: "currency_mismatch",
			Detail: fmt.Sprintf("account %s is in %s, event is in %s", tx.AccountId, currency, tx.Currency)}
	}
	// Only debits can be refused for lack of funds; a credit is always
	// accepted, even into an account that is already negative.
	newBalance := balance + delta
	if delta < 0 && newBalance < 0 {
		return false, &Rejection{Reason: "insufficient_funds",
			Detail: fmt.Sprintf("account %s has %d, event needs %d", tx.AccountId, balance, -delta)}
	}

	createdAt := tx.CreatedAt.AsTime()
	if exists {
		_, err = dbtx.Exec(ctx, updateBalance, tx.AccountId, newBalance, tx.EventId, createdAt)
	} else {
		tag, err = dbtx.Exec(ctx, createAccount, tx.AccountId, tx.Currency, newBalance, tx.EventId, createdAt)
		if err == nil && tag.RowsAffected() == 0 {
			// Another processor created the account between our SELECT and
			// INSERT; retrying will take the row lock path.
			return false, errors.New("account created concurrently")
		}
	}
	if err != nil {
		return false, fmt.Errorf("update balance: %w", err)
	}
	return true, dbtx.Commit(ctx)
}

// isPermanent reports whether retrying err can never succeed.
func isPermanent(err error) bool {
	var rej *Rejection
	if errors.As(err, &rej) {
		return true
	}
	// Class 22 (data exception) and 23 (integrity constraint violation) are
	// caused by the event's contents, not by the database being unavailable.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		class := pgErr.Code[:2]
		return class == "22" || class == "23"
	}
	return false
}
