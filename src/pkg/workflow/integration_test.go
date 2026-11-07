//go:build integration

package workflow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

var db *pgxpool.Pool

func TestMain(m *testing.M) {
	os.Exit(runWithPostgres(m))
}

func runWithPostgres(m *testing.M) int {
	ctx := context.Background()
	files, _ := filepath.Glob("../../../migrations/*.up.sql")
	sort.Strings(files)
	pgC, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("fintech"), tcpostgres.WithUsername("fintech"), tcpostgres.WithPassword("fintech"),
		tcpostgres.WithInitScripts(files...),
		tcpostgres.BasicWaitStrategies())
	if err != nil {
		fmt.Fprintf(os.Stderr, "start Postgres: %v\n", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(pgC) }()
	url, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Postgres URL: %v\n", err)
		return 1
	}
	if db, err = pgxpool.New(ctx, url); err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		return 1
	}
	defer db.Close()
	return m.Run()
}

type fakeStarter struct {
	mu      sync.Mutex
	started []SettlementInput
	failAt  int // fail on this call (1-based); 0 means never
	calls   int
}

func (f *fakeStarter) Start(_ context.Context, in SettlementInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls == f.failAt {
		return errors.New("temporal unavailable")
	}
	f.started = append(f.started, in)
	return nil
}

// addPending inserts settlements, oldest first, and returns their event IDs.
func addPending(t *testing.T, n int) []string {
	t.Helper()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "DELETE FROM settlements"); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, n)
	for i := range ids {
		ids[i] = uuid.NewString()
		if _, err := db.Exec(ctx, `INSERT INTO settlements (event_id, account_id, amount_minor, currency, created_at)
			VALUES ($1, 'acct-1', $2, 'USD', now() + make_interval(secs => $3))`, ids[i], 100+i, i); err != nil {
			t.Fatal(err)
		}
	}
	return ids
}

func statuses(t *testing.T) map[string]int {
	t.Helper()
	rows, err := db.Query(context.Background(), "SELECT status, count(*) FROM settlements GROUP BY status")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]int{}
	for rows.Next() {
		var s string
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			t.Fatal(err)
		}
		got[s] = n
	}
	return got
}

func TestRelayStartsPendingSettlementsOnce(t *testing.T) {
	ids := addPending(t, 5)
	starter := &fakeStarter{}
	relay := &Relay{Pool: db, Starter: starter}

	n, err := relay.RelayOnce(context.Background())
	if err != nil || n != 5 {
		t.Fatalf("RelayOnce = %d, %v; want 5, nil", n, err)
	}
	if got := statuses(t); got["started"] != 5 {
		t.Errorf("statuses = %v, want 5 started", got)
	}
	for i, in := range starter.started {
		if in.EventID != ids[i] {
			t.Errorf("start %d = %s, want oldest first (%s)", i, in.EventID, ids[i])
		}
		if in.Currency != "USD" || in.AccountID != "acct-1" {
			t.Errorf("input = %+v", in)
		}
	}

	if n, err := relay.RelayOnce(context.Background()); err != nil || n != 0 {
		t.Errorf("second RelayOnce = %d, %v; want nothing left to do", n, err)
	}
	if len(starter.started) != 5 {
		t.Errorf("started %d workflows, want 5", len(starter.started))
	}
}

// When Temporal goes down partway, what started is recorded and the rest wait.
func TestRelayKeepsUnstartedSettlementsPending(t *testing.T) {
	addPending(t, 5)
	failing := &fakeStarter{failAt: 3}
	n, err := (&Relay{Pool: db, Starter: failing}).RelayOnce(context.Background())
	if err == nil || n != 2 {
		t.Fatalf("RelayOnce = %d, %v; want 2 and an error", n, err)
	}
	if got := statuses(t); got["started"] != 2 || got["pending"] != 3 {
		t.Errorf("statuses = %v, want 2 started and 3 pending", got)
	}

	recovered := &fakeStarter{}
	if n, err := (&Relay{Pool: db, Starter: recovered}).RelayOnce(context.Background()); err != nil || n != 3 {
		t.Fatalf("RelayOnce after recovery = %d, %v; want 3, nil", n, err)
	}
	if got := statuses(t); got["started"] != 5 {
		t.Errorf("statuses = %v, want all 5 started", got)
	}
}

func TestPostgresOutcomesRecordsStatus(t *testing.T) {
	ids := addPending(t, 1)
	store, err := NewPostgresOutcomes(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordOutcome(context.Background(), ids[0], StatusReversed, "declined"); err != nil {
		t.Fatal(err)
	}
	var status, detail string
	err = db.QueryRow(context.Background(), "SELECT status, detail FROM settlements WHERE event_id = $1", ids[0]).Scan(&status, &detail)
	if err != nil || status != StatusReversed || detail != "declined" {
		t.Errorf("got %q/%q (%v), want reversed/declined", status, detail, err)
	}
	if err := store.RecordOutcome(context.Background(), "missing", StatusSettled, ""); err == nil {
		t.Error("recording an unknown settlement should fail so the activity retries")
	}
}
