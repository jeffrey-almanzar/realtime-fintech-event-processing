package processor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

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

// BatchItem is one validated event to apply. Items are applied in slice order.
type BatchItem struct {
	Tx        *fintechv1.Transaction
	Delta     int64
	Partition int32
	Offset    int64
}

// ItemResult says what happened to one BatchItem.
type ItemResult struct {
	Applied   bool       // false with a nil Rejection means a duplicate
	Rejection *Rejection // a business rule refused the event
}

const lockAccounts = `
SELECT account_id, balance_minor, currency FROM accounts
WHERE account_id = ANY($1) ORDER BY account_id FOR UPDATE`

const selectProcessed = `
SELECT event_id FROM processed_events WHERE event_id = ANY($1)`

const insertEvents = `
INSERT INTO processed_events
    (event_id, account_id, type, amount_minor, currency, created_at, kafka_partition, kafka_offset)
SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::bigint[], $5::text[], $6::timestamptz[], $7::int[], $8::bigint[])
ON CONFLICT (event_id) DO NOTHING`

// last_event_* let readers tie a balance change to the event that caused it
// (and measure latency up to the push to clients).
const updateBalances = `
UPDATE accounts a
SET balance_minor = u.balance, updated_at = now(), last_event_id = u.event_id, last_event_at = u.event_at
FROM unnest($1::text[], $2::bigint[], $3::text[], $4::timestamptz[]) AS u(account_id, balance, event_id, event_at)
WHERE a.account_id = u.account_id`

const createAccounts = `
INSERT INTO accounts (account_id, currency, balance_minor, last_event_id, last_event_at)
SELECT * FROM unnest($1::text[], $2::text[], $3::bigint[], $4::text[], $5::timestamptz[])
ON CONFLICT (account_id) DO NOTHING`

// A withdrawal's settlement is recorded with its debit, in one transaction, so
// neither can exist without the other. See migrations/000003_settlements.up.sql.
const insertSettlements = `
INSERT INTO settlements (event_id, account_id, amount_minor, currency)
SELECT * FROM unnest($1::text[], $2::text[], $3::bigint[], $4::text[])
ON CONFLICT (event_id) DO NOTHING`

// account is an account's state while a batch is being applied in memory.
type account struct {
	balance   int64
	currency  string
	exists    bool // has a row in the accounts table
	changed   bool
	lastEvent string
	lastAt    time.Time
}

// ApplyBatch applies items in one database transaction, which costs a fixed
// number of round trips and one commit however many events it holds.
//
// beforeCommit runs after every item is decided but before the commit, while
// the account rows are still locked. The caller publishes rejected events to
// the DLQ there: if we crash before the commit, the batch is redelivered and
// decided identically. Publishing after the commit could instead re-decide a
// rejected event against a balance that already includes later events.
func (s *Store) ApplyBatch(ctx context.Context, items []BatchItem, beforeCommit func([]ItemResult) error) ([]ItemResult, error) {
	dbtx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = dbtx.Rollback(ctx) }() // no-op after Commit

	accounts, processed, err := loadBatchState(ctx, dbtx, items)
	if err != nil {
		return nil, err
	}
	results := decide(items, accounts, processed)
	if err := writeBatch(ctx, dbtx, items, results, accounts); err != nil {
		return nil, err
	}
	if err := beforeCommit(results); err != nil {
		return nil, err
	}
	if err := dbtx.Commit(ctx); err != nil {
		return nil, err
	}
	return results, nil
}

// loadBatchState locks the batch's accounts and then finds events that were
// already applied. The order matters: another processor that briefly owns the
// partition during a rebalance must hold the same row locks to apply these
// events, so once we hold them its committed work is visible to our check.
func loadBatchState(ctx context.Context, dbtx pgx.Tx, items []BatchItem) (map[string]*account, map[string]bool, error) {
	accounts := make(map[string]*account, len(items))
	ids := make([]string, 0, len(items))
	eventIDs := make([]string, len(items))
	for i, it := range items {
		eventIDs[i] = it.Tx.EventId
		if accounts[it.Tx.AccountId] == nil {
			accounts[it.Tx.AccountId] = &account{}
			ids = append(ids, it.Tx.AccountId)
		}
	}

	batch := &pgx.Batch{}
	batch.Queue(lockAccounts, ids)
	batch.Queue(selectProcessed, eventIDs)
	results := dbtx.SendBatch(ctx, batch)
	defer func() { _ = results.Close() }()

	rows, err := results.Query()
	if err != nil {
		return nil, nil, fmt.Errorf("lock accounts: %w", err)
	}
	for rows.Next() {
		var id, currency string
		var balance int64
		if err := rows.Scan(&id, &balance, &currency); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("lock accounts: %w", err)
		}
		*accounts[id] = account{balance: balance, currency: currency, exists: true}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("lock accounts: %w", err)
	}

	rows, err = results.Query()
	if err != nil {
		return nil, nil, fmt.Errorf("find processed events: %w", err)
	}
	processed, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, nil, fmt.Errorf("find processed events: %w", err)
	}
	seen := make(map[string]bool, len(processed))
	for _, id := range processed {
		seen[id] = true
	}
	return accounts, seen, nil
}

// decide walks the items in order against in-memory balances, so each event
// sees the effect of the ones before it exactly as if applied one at a time.
func decide(items []BatchItem, accounts map[string]*account, processed map[string]bool) []ItemResult {
	results := make([]ItemResult, len(items))
	for i, it := range items {
		if processed[it.Tx.EventId] {
			continue // duplicate, either from an earlier batch or earlier in this one
		}
		a := accounts[it.Tx.AccountId]
		if rej := checkRules(it, a); rej != nil {
			results[i].Rejection = rej
			continue
		}
		if !a.exists && !a.changed {
			a.currency = it.Tx.Currency // a new account takes its first applied event's currency
		}
		a.balance += it.Delta
		a.changed = true
		a.lastEvent = it.Tx.EventId
		a.lastAt = it.Tx.CreatedAt.AsTime()
		processed[it.Tx.EventId] = true
		results[i].Applied = true
	}
	return results
}

func checkRules(it BatchItem, a *account) *Rejection {
	if (a.exists || a.changed) && a.currency != it.Tx.Currency {
		return &Rejection{Reason: "currency_mismatch",
			Detail: fmt.Sprintf("account %s is in %s, event is in %s", it.Tx.AccountId, a.currency, it.Tx.Currency)}
	}
	// Only debits can be refused for lack of funds; a credit is always
	// accepted, even into an account that is already negative.
	if it.Delta < 0 && a.balance+it.Delta < 0 {
		return &Rejection{Reason: "insufficient_funds",
			Detail: fmt.Sprintf("account %s has %d, event needs %d", it.Tx.AccountId, a.balance, -it.Delta)}
	}
	if it.Delta > 0 && a.balance > math.MaxInt64-it.Delta {
		return &Rejection{Reason: "balance_overflow",
			Detail: fmt.Sprintf("account %s has %d, adding %d overflows", it.Tx.AccountId, a.balance, it.Delta)}
	}
	return nil
}

// writeBatch sends all the writes in one round trip.
func writeBatch(ctx context.Context, dbtx pgx.Tx, items []BatchItem, results []ItemResult, accounts map[string]*account) error {
	var ev struct {
		ids, accounts, types, currencies []string
		amounts, offsets                 []int64
		createdAt                        []time.Time
		partitions                       []int32
	}
	for i, it := range items {
		if !results[i].Applied {
			continue
		}
		ev.ids = append(ev.ids, it.Tx.EventId)
		ev.accounts = append(ev.accounts, it.Tx.AccountId)
		ev.types = append(ev.types, it.Tx.Type.String())
		ev.amounts = append(ev.amounts, it.Tx.AmountMinor)
		ev.currencies = append(ev.currencies, it.Tx.Currency)
		ev.createdAt = append(ev.createdAt, it.Tx.CreatedAt.AsTime())
		ev.partitions = append(ev.partitions, it.Partition)
		ev.offsets = append(ev.offsets, it.Offset)
	}
	if len(ev.ids) == 0 {
		return nil
	}

	var st struct {
		ids, accounts, currencies []string
		amounts                   []int64
	}
	for i, it := range items {
		if results[i].Applied && it.Tx.Type == fintechv1.TransactionType_TRANSACTION_TYPE_WITHDRAWAL {
			st.ids = append(st.ids, it.Tx.EventId)
			st.accounts = append(st.accounts, it.Tx.AccountId)
			st.amounts = append(st.amounts, it.Tx.AmountMinor)
			st.currencies = append(st.currencies, it.Tx.Currency)
		}
	}

	var upd, ins struct {
		ids, currencies, lastEvents []string
		balances                    []int64
		lastAts                     []time.Time
	}
	for id, a := range accounts {
		if !a.changed {
			continue
		}
		t := &ins
		if a.exists {
			t = &upd
		}
		t.ids = append(t.ids, id)
		t.currencies = append(t.currencies, a.currency)
		t.balances = append(t.balances, a.balance)
		t.lastEvents = append(t.lastEvents, a.lastEvent)
		t.lastAts = append(t.lastAts, a.lastAt)
	}

	batch := &pgx.Batch{}
	batch.Queue(insertEvents, ev.ids, ev.accounts, ev.types, ev.amounts, ev.currencies, ev.createdAt, ev.partitions, ev.offsets)
	batch.Queue(updateBalances, upd.ids, upd.balances, upd.lastEvents, upd.lastAts)
	batch.Queue(createAccounts, ins.ids, ins.currencies, ins.balances, ins.lastEvents, ins.lastAts)
	batch.Queue(insertSettlements, st.ids, st.accounts, st.amounts, st.currencies)
	sent := dbtx.SendBatch(ctx, batch)
	defer func() { _ = sent.Close() }()

	tag, err := sent.Exec()
	if err != nil {
		return fmt.Errorf("record events: %w", err)
	}
	// Fewer rows than expected means another processor got there first. Fail
	// the transaction so a retry re-reads the state under locks.
	if tag.RowsAffected() != int64(len(ev.ids)) {
		return errors.New("events applied concurrently")
	}
	if _, err := sent.Exec(); err != nil {
		return fmt.Errorf("update balances: %w", err)
	}
	tag, err = sent.Exec()
	if err != nil {
		return fmt.Errorf("create accounts: %w", err)
	}
	if tag.RowsAffected() != int64(len(ins.ids)) {
		return errors.New("accounts created concurrently")
	}
	if _, err := sent.Exec(); err != nil {
		return fmt.Errorf("record settlements: %w", err)
	}
	return nil
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
