//go:build integration

package query

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

var (
	dbURL string
	db    *pgxpool.Pool
)

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
	if dbURL, err = pgC.ConnectionString(ctx, "sslmode=disable"); err != nil {
		fmt.Fprintf(os.Stderr, "Postgres URL: %v\n", err)
		return 1
	}
	if db, err = pgxpool.New(ctx, dbURL); err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		return 1
	}
	defer db.Close()
	return m.Run()
}

// startServer runs the Listener and HTTP server like cmd/query does.
func startServer(t *testing.T) *httptest.Server {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	store, err := NewStore(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	hub := NewHub()
	listener, err := NewListener(dbURL, hub, store)
	if err != nil {
		t.Fatal(err)
	}
	go listener.Run(ctx)

	server, err := NewServer(store, hub, "http://localhost:8081")
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	time.Sleep(300 * time.Millisecond) // let the listener connect
	return ts
}

func exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := db.Exec(context.Background(), sql, args...); err != nil {
		t.Fatal(err)
	}
}

type sseEvent struct {
	name string
	data string
}

// stream opens an SSE connection and returns a channel of parsed events.
func stream(t *testing.T, url string) <-chan sseEvent {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	events := make(chan sseEvent, 16)
	go func() {
		defer func() { _ = resp.Body.Close() }()
		var ev sseEvent
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			switch {
			case strings.HasPrefix(line, "event: "):
				ev.name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				ev.data = strings.TrimPrefix(line, "data: ")
			case line == "" && ev.name != "":
				events <- ev
				ev = sseEvent{}
			}
		}
	}()
	return events
}

func nextBalance(t *testing.T, events <-chan sseEvent) Account {
	t.Helper()
	select {
	case ev := <-events:
		if ev.name != "balance" {
			t.Fatalf("got %q event, want balance", ev.name)
		}
		var a Account
		if err := json.Unmarshal([]byte(ev.data), &a); err != nil {
			t.Fatal(err)
		}
		return a
	case <-time.After(5 * time.Second):
		t.Fatal("no balance event within 5s")
		return Account{}
	}
}

func TestStreamSendsSnapshotThenLiveUpdates(t *testing.T) {
	ts := startServer(t)
	id := "acct-" + uuid.NewString()[:8]
	exec(t, "INSERT INTO accounts (account_id, currency, balance_minor) VALUES ($1, 'USD', 1000)", id)

	events := stream(t, ts.URL+"/accounts/"+id+"/events")
	snap := nextBalance(t, events)
	if snap.BalanceMinor != 1000 || snap.Version != 1 || snap.Currency != "USD" {
		t.Fatalf("snapshot = %+v, want balance 1000 version 1 USD", snap)
	}

	eventID := uuid.NewString()
	exec(t, "UPDATE accounts SET balance_minor = 1500, last_event_id = $2, last_event_at = now() WHERE account_id = $1", id, eventID)
	got := nextBalance(t, events)
	if got.BalanceMinor != 1500 || got.Version != 2 {
		t.Errorf("update = %+v, want balance 1500 version 2", got)
	}
	if got.LastEventID == nil || *got.LastEventID != eventID || got.LastEventAt == nil {
		t.Errorf("update missing last event: %+v", got)
	}
}

func TestStreamForNewAccountReportsNotFoundThenFirstBalance(t *testing.T) {
	ts := startServer(t)
	id := "acct-" + uuid.NewString()[:8]

	events := stream(t, ts.URL+"/accounts/"+id+"/events")
	select {
	case ev := <-events:
		if ev.name != "not_found" {
			t.Fatalf("first event = %q, want not_found", ev.name)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no initial event")
	}

	exec(t, "INSERT INTO accounts (account_id, currency, balance_minor) VALUES ($1, 'EUR', 42)", id)
	if got := nextBalance(t, events); got.BalanceMinor != 42 || got.Currency != "EUR" {
		t.Errorf("first balance = %+v, want 42 EUR", got)
	}
}

func TestStreamDoesNotLeakOtherAccounts(t *testing.T) {
	ts := startServer(t)
	mine, other := "acct-"+uuid.NewString()[:8], "acct-"+uuid.NewString()[:8]
	exec(t, "INSERT INTO accounts (account_id, currency, balance_minor) VALUES ($1, 'USD', 1), ($2, 'USD', 1)", mine, other)

	events := stream(t, ts.URL+"/accounts/"+mine+"/events")
	nextBalance(t, events) // snapshot
	exec(t, "UPDATE accounts SET balance_minor = 2 WHERE account_id = $1", other)
	exec(t, "UPDATE accounts SET balance_minor = 3 WHERE account_id = $1", mine)

	if got := nextBalance(t, events); got.AccountID != mine || got.BalanceMinor != 3 {
		t.Errorf("got %+v, want only %s's update", got, mine)
	}
}

func TestTransactionsIncludeSettlementStatus(t *testing.T) {
	ts := startServer(t)
	id, eventID := "acct-"+uuid.NewString()[:8], uuid.NewString()
	exec(t, `INSERT INTO processed_events
		(event_id, account_id, type, amount_minor, currency, created_at, kafka_partition, kafka_offset)
		VALUES ($1, $2, 'TRANSACTION_TYPE_WITHDRAWAL', 100, 'USD', now(), 0, 0)`, eventID, id)
	exec(t, `INSERT INTO settlements (event_id, account_id, amount_minor, currency, status)
		VALUES ($1, $2, 100, 'USD', 'reversed')`, eventID, id)

	var p struct {
		Transactions []Transaction `json:"transactions"`
	}
	getJSON(t, ts.URL+"/accounts/"+id+"/transactions", &p)
	if len(p.Transactions) != 1 || p.Transactions[0].Settlement == nil || *p.Transactions[0].Settlement != "reversed" {
		t.Errorf("transactions = %+v, want one with settlement reversed", p.Transactions)
	}
}

func getJSON(t *testing.T, url string, into any) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if into != nil && resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
			t.Fatal(err)
		}
	}
	return resp.StatusCode
}

func TestAccountEndpoint(t *testing.T) {
	ts := startServer(t)
	id := "acct-" + uuid.NewString()[:8]
	if code := getJSON(t, ts.URL+"/accounts/"+id, nil); code != http.StatusNotFound {
		t.Errorf("unknown account: status %d, want 404", code)
	}

	exec(t, "INSERT INTO accounts (account_id, currency, balance_minor) VALUES ($1, 'GBP', 750)", id)
	var a Account
	if code := getJSON(t, ts.URL+"/accounts/"+id, &a); code != http.StatusOK || a.BalanceMinor != 750 || a.Currency != "GBP" {
		t.Errorf("status %d, account %+v; want 200 with 750 GBP", code, a)
	}
}

func TestTransactionsArePagedNewestFirst(t *testing.T) {
	ts := startServer(t)
	id := "acct-" + uuid.NewString()[:8]
	base := time.Now().Add(-time.Hour)
	for i := range 5 {
		txType := "TRANSACTION_TYPE_DEPOSIT"
		if i == 4 {
			txType = "TRANSACTION_TYPE_WITHDRAWAL"
		}
		exec(t, `INSERT INTO processed_events
			(event_id, account_id, type, amount_minor, currency, created_at, kafka_partition, kafka_offset)
			VALUES ($1, $2, $3, $4, 'USD', $5, 0, $6)`,
			uuid.NewString(), id, txType, (i+1)*100, base.Add(time.Duration(i)*time.Minute), i)
	}

	type page struct {
		Transactions []Transaction `json:"transactions"`
		NextCursor   string        `json:"next_cursor"`
	}
	var p1, p2, p3 page
	getJSON(t, ts.URL+"/accounts/"+id+"/transactions?limit=2", &p1)
	getJSON(t, ts.URL+"/accounts/"+id+"/transactions?limit=2&cursor="+p1.NextCursor, &p2)
	getJSON(t, ts.URL+"/accounts/"+id+"/transactions?limit=2&cursor="+p2.NextCursor, &p3)

	var amounts []int64
	for _, p := range []page{p1, p2, p3} {
		for _, tx := range p.Transactions {
			amounts = append(amounts, tx.AmountMinor)
		}
	}
	want := []int64{-500, 400, 300, 200, 100}
	if fmt.Sprint(amounts) != fmt.Sprint(want) {
		t.Errorf("amounts across pages = %v, want %v (newest first, debits negative)", amounts, want)
	}
	if p1.Transactions[0].Type != "withdrawal" {
		t.Errorf("type = %q, want withdrawal", p1.Transactions[0].Type)
	}
	if p3.NextCursor != "" {
		t.Errorf("last page next_cursor = %q, want empty", p3.NextCursor)
	}

	if code := getJSON(t, ts.URL+"/accounts/"+id+"/transactions?cursor=garbage", nil); code != http.StatusBadRequest {
		t.Errorf("bad cursor: status %d, want 400", code)
	}
	if code := getJSON(t, ts.URL+"/accounts/"+id+"/transactions?limit=1000", nil); code != http.StatusBadRequest {
		t.Errorf("limit 1000: status %d, want 400", code)
	}
}
