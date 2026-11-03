package main

import (
	"context"
	"fmt"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
)

// signedAmount returns how much tx changes the account balance.
func signedAmount(tx *fintechv1.Transaction) (int64, error) {
	switch tx.Type {
	case fintechv1.TransactionType_TRANSACTION_TYPE_DEPOSIT,
		fintechv1.TransactionType_TRANSACTION_TYPE_TRANSFER_IN:
		return tx.AmountMinor, nil
	case fintechv1.TransactionType_TRANSACTION_TYPE_WITHDRAWAL,
		fintechv1.TransactionType_TRANSACTION_TYPE_TRANSFER_OUT:
		return -tx.AmountMinor, nil
	default:
		return 0, fmt.Errorf("unsupported transaction type %s", tx.Type)
	}
}

// Store applies transactions to account balances in Postgres.
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

const insertEvent = `
INSERT INTO processed_events
    (event_id, account_id, type, amount_minor, currency, created_at, kafka_partition, kafka_offset)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (event_id) DO NOTHING`

const upsertBalance = `
INSERT INTO accounts (account_id, currency, balance_minor)
VALUES ($1, $2, $3)
ON CONFLICT (account_id) DO UPDATE
SET balance_minor = accounts.balance_minor + EXCLUDED.balance_minor,
    updated_at    = now()`

// Apply records the event and updates the balance in one database
// transaction. It reports false when the event was already applied (a
// redelivery), in which case the balance is left untouched.
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

	if _, err := dbtx.Exec(ctx, upsertBalance, tx.AccountId, tx.Currency, delta); err != nil {
		return false, fmt.Errorf("update balance: %w", err)
	}
	return true, dbtx.Commit(ctx)
}
