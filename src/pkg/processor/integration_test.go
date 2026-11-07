//go:build integration

// Integration tests: run real Kafka and Postgres with Testcontainers.
//
//	make test-integration

package processor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/config"
)

const partitions = 3

var (
	brokers string
	dbURL   string
	db      *pgxpool.Pool
)

func TestMain(m *testing.M) {
	os.Exit(runWithContainers(m))
}

func runWithContainers(m *testing.M) int {
	ctx := context.Background()

	kafkaC, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0", tckafka.WithClusterID("processor-test"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "start Kafka: %v\n", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(kafkaC) }()
	addrs, err := kafkaC.Brokers(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Kafka brokers: %v\n", err)
		return 1
	}
	brokers = addrs[0]

	pgC, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("fintech"), tcpostgres.WithUsername("fintech"), tcpostgres.WithPassword("fintech"),
		tcpostgres.WithInitScripts(upMigrations()...),
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
		fmt.Fprintf(os.Stderr, "connect to Postgres: %v\n", err)
		return 1
	}
	defer db.Close()

	return m.Run()
}

// upMigrations returns the repo's *.up.sql files in order.
func upMigrations() []string {
	files, err := filepath.Glob("../../../migrations/*.up.sql")
	if err != nil || len(files) == 0 {
		panic(fmt.Sprintf("no migrations found: %v", err))
	}
	sort.Strings(files)
	return files
}

// env is an isolated topic pair, consumer group, and account prefix.
type env struct {
	t      *testing.T
	kafka  config.Kafka
	prefix string
	prod   *kafka.Producer
}

func newEnv(t *testing.T) *env {
	t.Helper()
	id := uuid.NewString()[:8]
	e := &env{
		t: t,
		kafka: config.Kafka{
			BootstrapServers: brokers,
			Topic:            "tx-" + id,
			DLQTopic:         "tx-" + id + ".dlq",
			GroupID:          "processor-" + id,
		},
		prefix: "acct-" + id + "-",
	}

	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	results, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{
		{Topic: e.kafka.Topic, NumPartitions: partitions, ReplicationFactor: 1},
		{Topic: e.kafka.DLQTopic, NumPartitions: 1, ReplicationFactor: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range results {
		if r.Error.Code() != kafka.ErrNoError {
			t.Fatalf("create topic %s: %v", r.Topic, r.Error)
		}
	}

	if e.prod, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.prod.Close)
	return e
}

func (e *env) config() Config {
	return Config{
		Kafka:           e.kafka,
		DatabaseURL:     dbURL,
		Retry:           RetryPolicy{MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond},
		PartitionBuffer: 100,
		MaxBatch:        50,
		// Faster group joins and rebalances for tests.
		ConsumerOverrides: kafka.ConfigMap{"session.timeout.ms": 6000, "heartbeat.interval.ms": 500},
	}
}

// start runs a processor in the background and returns a function that
// stops it and returns Run's result.
func (e *env) start(cfg Config) (stop func() error) {
	e.t.Helper()
	p, err := New(context.Background(), cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	var stopped atomic.Bool
	stop = func() error {
		if stopped.Swap(true) {
			return nil
		}
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(30 * time.Second):
			e.t.Fatal("processor did not stop")
			return nil
		}
	}
	// If Run already returned on its own (a crash), stop returns its error.
	e.t.Cleanup(func() { _ = stop() })
	return stop
}

func (e *env) tx(account string, txType fintechv1.TransactionType, amount int64) *fintechv1.Transaction {
	return &fintechv1.Transaction{
		EventId:     uuid.NewString(),
		AccountId:   e.prefix + account,
		Type:        txType,
		AmountMinor: amount,
		Currency:    "USD",
		CreatedAt:   timestamppb.Now(),
	}
}

func (e *env) publish(txs ...*fintechv1.Transaction) {
	e.t.Helper()
	for _, tx := range txs {
		value, err := proto.Marshal(tx)
		if err != nil {
			e.t.Fatal(err)
		}
		e.publishRaw(tx.AccountId, value)
	}
}

func (e *env) publishRaw(key string, value []byte) {
	e.t.Helper()
	delivery := make(chan kafka.Event, 1)
	err := e.prod.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &e.kafka.Topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          value,
	}, delivery)
	if err != nil {
		e.t.Fatal(err)
	}
	if m := (<-delivery).(*kafka.Message); m.TopicPartition.Error != nil {
		e.t.Fatal(m.TopicPartition.Error)
	}
}

func (e *env) balance(account string) (int64, bool) {
	e.t.Helper()
	var b int64
	err := db.QueryRow(context.Background(),
		"SELECT balance_minor FROM accounts WHERE account_id = $1", e.prefix+account).Scan(&b)
	if err != nil {
		return 0, false
	}
	return b, true
}

func (e *env) processedCount() int {
	e.t.Helper()
	var n int
	if err := db.QueryRow(context.Background(),
		"SELECT count(*) FROM processed_events WHERE account_id LIKE $1", e.prefix+"%").Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

func (e *env) totalBalance() int64 {
	e.t.Helper()
	var n int64
	if err := db.QueryRow(context.Background(),
		"SELECT coalesce(sum(balance_minor), 0) FROM accounts WHERE account_id LIKE $1", e.prefix+"%").Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

// dlqMessages reads the DLQ topic until want messages arrive or it times out.
func (e *env) dlqMessages(want int) []*kafka.Message {
	e.t.Helper()
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          "dlq-reader-" + uuid.NewString()[:8],
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		e.t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if err := c.Subscribe(e.kafka.DLQTopic, nil); err != nil {
		e.t.Fatal(err)
	}

	var msgs []*kafka.Message
	deadline := time.Now().Add(30 * time.Second)
	for len(msgs) < want && time.Now().Before(deadline) {
		if m, err := c.ReadMessage(500 * time.Millisecond); err == nil {
			msgs = append(msgs, m)
		}
	}
	return msgs
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func header(m *kafka.Message, key string) string {
	for _, h := range m.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

var (
	deposit    = fintechv1.TransactionType_TRANSACTION_TYPE_DEPOSIT
	withdrawal = fintechv1.TransactionType_TRANSACTION_TYPE_WITHDRAWAL
)

// Each account gets deposit 100, withdraw 100, deposit 50. Applied out of
// order, the withdrawal would overdraw and be dead-lettered, so a clean run
// shows per-account ordering holds while partitions run concurrently.
func TestAppliesEventsInOrderPerAccount(t *testing.T) {
	e := newEnv(t)
	const accounts = 30
	for i := range accounts {
		a := fmt.Sprint(i)
		e.publish(e.tx(a, deposit, 100), e.tx(a, withdrawal, 100), e.tx(a, deposit, 50))
	}

	// Dead-lettered events aren't recorded in processed_events, so reaching
	// the full count also means nothing was rejected.
	stop := e.start(e.config())
	eventually(t, "all events processed", func() bool { return e.processedCount() == 3*accounts })
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}

	for i := range accounts {
		if b, _ := e.balance(fmt.Sprint(i)); b != 50 {
			t.Errorf("account %d balance = %d, want 50", i, b)
		}
	}
}

func TestDuplicateDeliveryIsAppliedOnce(t *testing.T) {
	e := newEnv(t)
	tx := e.tx("dup", deposit, 700)
	e.publish(tx, tx, tx)
	marker := e.tx("marker", deposit, 1)
	e.publish(marker)

	stop := e.start(e.config())
	eventually(t, "marker processed", func() bool { _, ok := e.balance("marker"); return ok })
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}

	if b, _ := e.balance("dup"); b != 700 {
		t.Errorf("balance = %d, want 700 (applied once)", b)
	}
	if n := e.processedCount(); n != 2 {
		t.Errorf("processed_events = %d, want 2", n)
	}
}

// The processor "crashes" after an event is written to Postgres but before its
// offset is stored. A restarted processor receives that event again and must
// not apply it twice.
func TestCrashBetweenWriteAndOffsetCommit(t *testing.T) {
	e := newEnv(t)
	const n = 40
	var want int64
	for i := range n {
		e.publish(e.tx(fmt.Sprint(i%5), deposit, int64(i+1)))
		want += int64(i + 1)
	}

	var handled atomic.Int32
	crashCfg := e.config()
	crashCfg.AfterHandle = func(*kafka.Message) error {
		if handled.Add(1) == n/2 {
			return errors.New("simulated crash")
		}
		return nil
	}
	stop := e.start(crashCfg)
	eventually(t, "simulated crash", func() bool { return handled.Load() >= n/2 })
	if err := stop(); err == nil || err.Error() != "simulated crash" {
		t.Fatalf("Run = %v, want the simulated crash", err)
	}

	stop = e.start(e.config())
	eventually(t, "all events processed after restart", func() bool { return e.processedCount() == n })
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if got := e.totalBalance(); got != want {
		t.Errorf("total balance = %d, want %d", got, want)
	}
}

// Worst case: every committed offset is lost and the whole topic is
// reprocessed. Balances must not change.
func TestFullReplayIsIdempotent(t *testing.T) {
	e := newEnv(t)
	for i := range 20 {
		e.publish(e.tx(fmt.Sprint(i%4), deposit, 10))
	}

	stop := e.start(e.config())
	eventually(t, "first pass", func() bool { return e.processedCount() == 20 })
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
	before := e.totalBalance()

	var replayed atomic.Int32
	replayCfg := e.config()
	replayCfg.Kafka.GroupID += "-replay" // a new group starts from the beginning
	replayCfg.AfterHandle = func(*kafka.Message) error { replayed.Add(1); return nil }
	stop = e.start(replayCfg)
	eventually(t, "replay", func() bool { return replayed.Load() == 20 })
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}

	if after := e.totalBalance(); after != before || before != 200 {
		t.Errorf("total balance before=%d after=%d, want 200 both times", before, after)
	}
}

func TestInvalidEventsGoToDLQ(t *testing.T) {
	e := newEnv(t)
	e.publishRaw(e.prefix+"garbage", []byte{0xff, 0xff, 0xff})
	badCurrency := e.tx("cur", deposit, 100)
	badCurrency.Currency = "XYZ"
	e.publish(badCurrency)
	e.publish(e.tx("broke", withdrawal, 500))
	e.publish(e.tx("eur", deposit, 100))
	mismatch := e.tx("eur", deposit, 100)
	mismatch.Currency = "EUR"
	e.publish(mismatch)
	e.publish(e.tx("ok", deposit, 100))

	stop := e.start(e.config())
	eventually(t, "valid event after bad ones", func() bool { _, ok := e.balance("ok"); return ok })

	msgs := e.dlqMessages(4)
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
	reasons := map[string]int{}
	for _, m := range msgs {
		reasons[header(m, HeaderDLQReason)]++
		if header(m, HeaderDLQSourceTopic) != e.kafka.Topic || header(m, HeaderDLQError) == "" {
			t.Errorf("DLQ message missing source/error headers: %v", m.Headers)
		}
	}
	want := map[string]int{"malformed": 1, "invalid": 1, "insufficient_funds": 1, "currency_mismatch": 1}
	for reason, n := range want {
		if reasons[reason] != n {
			t.Errorf("DLQ reasons = %v, want %v", reasons, want)
			break
		}
	}
	if _, ok := e.balance("broke"); ok {
		t.Error("overdrawn account should not have been created")
	}
	if b, _ := e.balance("eur"); b != 100 {
		t.Errorf("USD account balance = %d, want 100 (EUR event rejected)", b)
	}
}

// Regression: credits were once refused when the balance stayed negative
// afterwards. Only debits may be refused for insufficient funds.
func TestCreditsAreAcceptedIntoNegativeAccount(t *testing.T) {
	e := newEnv(t)
	if _, err := db.Exec(context.Background(),
		"INSERT INTO accounts (account_id, currency, balance_minor) VALUES ($1, 'USD', -500)", e.prefix+"neg"); err != nil {
		t.Fatal(err)
	}
	e.publish(e.tx("neg", deposit, 200), e.tx("neg", withdrawal, 1))

	stop := e.start(e.config())
	msgs := e.dlqMessages(1)
	eventually(t, "deposit applied", func() bool { b, _ := e.balance("neg"); return b == -300 })
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if len(msgs) != 1 || header(msgs[0], HeaderDLQReason) != "insufficient_funds" {
		t.Errorf("want only the withdrawal dead-lettered as insufficient_funds, got %d messages", len(msgs))
	}
}

// Batches form when events are already waiting, as they are here: everything
// is published before the processor starts. A rejected event must not affect
// the rest of its batch, and later events must see earlier ones' effects.
func TestBatchDecidesEventsInOrder(t *testing.T) {
	e := newEnv(t)
	e.publish(
		e.tx("a", deposit, 100),
		e.tx("a", withdrawal, 500), // rejected
		e.tx("a", withdrawal, 60),  // allowed only after the deposit
		e.tx("a", deposit, 10),
	)

	stop := e.start(e.config())
	eventually(t, "batch applied", func() bool { b, _ := e.balance("a"); return b == 50 })
	msgs := e.dlqMessages(1)
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if len(msgs) != 1 || header(msgs[0], HeaderDLQReason) != "insufficient_funds" {
		t.Errorf("want one insufficient_funds in the DLQ, got %d messages", len(msgs))
	}
	if n := e.processedCount(); n != 3 {
		t.Errorf("processed_events = %d, want 3", n)
	}
}

// A withdrawal's settlement commits with its debit. A rejected withdrawal must
// not create one, and redelivery must not create a second.
func TestWithdrawalRecordsSettlementWithItsDebit(t *testing.T) {
	e := newEnv(t)
	applied := e.tx("s", withdrawal, 30)
	rejected := e.tx("s", withdrawal, 5000)
	e.publish(e.tx("s", deposit, 100), applied, applied, rejected)

	stop := e.start(e.config())
	eventually(t, "withdrawal applied", func() bool { b, _ := e.balance("s"); return b == 70 })
	e.dlqMessages(1)
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}

	var n int
	var status string
	err := db.QueryRow(context.Background(),
		"SELECT count(*), min(status) FROM settlements WHERE account_id = $1", e.prefix+"s").Scan(&n, &status)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || status != "pending" {
		t.Errorf("settlements = %d with status %q, want 1 pending", n, status)
	}
	var id string
	if err := db.QueryRow(context.Background(),
		"SELECT event_id FROM settlements WHERE account_id = $1", e.prefix+"s").Scan(&id); err != nil || id != applied.EventId {
		t.Errorf("settlement event = %q (%v), want %q", id, err, applied.EventId)
	}
}

// Regression: a new account took its currency from the first event in the
// batch even when that event was rejected, so the next event was refused.
func TestRejectedEventDoesNotSetNewAccountCurrency(t *testing.T) {
	e := newEnv(t)
	rejected := e.tx("new", withdrawal, 100)
	rejected.Currency = "EUR"
	e.publish(rejected, e.tx("new", deposit, 40))

	stop := e.start(e.config())
	eventually(t, "deposit applied", func() bool { b, _ := e.balance("new"); return b == 40 })
	msgs := e.dlqMessages(1)
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if len(msgs) != 1 || header(msgs[0], HeaderDLQReason) != "insufficient_funds" {
		t.Errorf("want only the withdrawal in the DLQ, got %d messages", len(msgs))
	}
}

// Postgres refuses a NUL byte in text, which fails the whole batch's write.
// Only the offending event may go to the DLQ.
func TestPoisonEventDoesNotFailItsBatch(t *testing.T) {
	e := newEnv(t)
	poison := e.tx("poison", deposit, 1)
	poison.AccountId = e.prefix + "a\x00b"
	var txs []*fintechv1.Transaction
	for i := range 10 {
		txs = append(txs, e.tx(fmt.Sprint(i%3), deposit, 5))
	}
	txs = append(txs[:5:5], append([]*fintechv1.Transaction{poison}, txs[5:]...)...)
	e.publish(txs...)

	stop := e.start(e.config())
	eventually(t, "valid events applied", func() bool { return e.processedCount() == 10 })
	msgs := e.dlqMessages(1)
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if len(msgs) != 1 || header(msgs[0], HeaderDLQReason) != "invalid" {
		t.Fatalf("want the poison event in the DLQ as invalid, got %d messages", len(msgs))
	}
	if got := e.totalBalance(); got != 50 {
		t.Errorf("total balance = %d, want 50", got)
	}
}

func TestBalanceOverflowIsRejected(t *testing.T) {
	e := newEnv(t)
	if _, err := db.Exec(context.Background(),
		"INSERT INTO accounts (account_id, currency, balance_minor) VALUES ($1, 'USD', 9223372036854775000)", e.prefix+"big"); err != nil {
		t.Fatal(err)
	}
	e.publish(e.tx("big", deposit, 1000))

	stop := e.start(e.config())
	msgs := e.dlqMessages(1)
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if len(msgs) != 1 || header(msgs[0], HeaderDLQReason) != "balance_overflow" {
		t.Errorf("want balance_overflow in the DLQ, got %d messages", len(msgs))
	}
	if b, _ := e.balance("big"); b != 9223372036854775000 {
		t.Errorf("balance changed to %d", b)
	}
}

// After a graceful stop, a new processor in the same group must not see any
// already-handled event again.
func TestGracefulShutdownCommitsOffsets(t *testing.T) {
	e := newEnv(t)
	for i := range 25 {
		e.publish(e.tx(fmt.Sprint(i%5), deposit, 1))
	}
	stop := e.start(e.config())
	eventually(t, "first batch", func() bool { return e.processedCount() == 25 })
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}

	var seen atomic.Int32
	cfg := e.config()
	cfg.AfterHandle = func(*kafka.Message) error { seen.Add(1); return nil }
	stop = e.start(cfg)
	e.publish(e.tx("after-restart", deposit, 1))
	eventually(t, "new event", func() bool { _, ok := e.balance("after-restart"); return ok })
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if got := seen.Load(); got != 1 {
		t.Errorf("restarted processor handled %d events, want 1 (only the new one)", got)
	}
}
