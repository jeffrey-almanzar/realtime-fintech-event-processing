// Load generator: sends transactions at a fixed rate and reports the
// client-side results. End-to-end latency is measured by the processor and
// shown in Grafana.
//
//	go run ./src/cmd/loadgen -rate 200 -duration 60s            # through the Event API
//	go run ./src/cmd/loadgen -rate 5000 -duration 60s -direct   # straight to Kafka
//
// -direct skips HTTP, which tests the Processor beyond what the Event API and
// this generator can drive together on one machine.
//
// With -record, the IDs of accepted events are written to a file (one per
// line) so you can check afterwards that every one of them was applied.

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fintechv1 "github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/gen/fintech/v1"
	"github.com/jeffrey-almanzar/realtime-fintech-event-processing/src/pkg/config"
)

// tick is how often the pacer releases requests. A ticker per request can't
// keep up at high rates because Go tickers drop ticks for slow receivers.
const tick = 10 * time.Millisecond

type event struct {
	ID          string
	AccountID   string
	Type        fintechv1.TransactionType
	AmountMinor int64
}

// sender delivers one event. Implementations report results to stats.
type sender func(event)

type stats struct {
	ok, failed, dropped atomic.Int64
	mu                  sync.Mutex
	latencies           []time.Duration
	accepted            []string
}

func (s *stats) success(id string, latency time.Duration) {
	s.ok.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if latency > 0 {
		s.latencies = append(s.latencies, latency)
	}
	s.accepted = append(s.accepted, id)
}

func main() {
	url := flag.String("url", "http://localhost:8081/transactions", "Event API endpoint")
	rate := flag.Int("rate", 100, "events per second")
	duration := flag.Duration("duration", 30*time.Second, "how long to send")
	accounts := flag.Int("accounts", 1000, "number of distinct account IDs")
	workers := flag.Int("workers", 256, "concurrent HTTP clients")
	debits := flag.Bool("debits", false, "include withdrawals and transfers out (some will be rejected for insufficient funds)")
	record := flag.String("record", "", "file to write accepted event IDs to")
	direct := flag.Bool("direct", false, "produce straight to Kafka instead of calling the Event API")
	linger := flag.Int("linger-ms", 1, "with -direct: how long the producer waits to fill a batch")
	flag.Parse()

	types := []fintechv1.TransactionType{
		fintechv1.TransactionType_TRANSACTION_TYPE_DEPOSIT,
		fintechv1.TransactionType_TRANSACTION_TYPE_TRANSFER_IN,
	}
	if *debits {
		types = append(types,
			fintechv1.TransactionType_TRANSACTION_TYPE_DEPOSIT,
			fintechv1.TransactionType_TRANSACTION_TYPE_DEPOSIT,
			fintechv1.TransactionType_TRANSACTION_TYPE_WITHDRAWAL,
			fintechv1.TransactionType_TRANSACTION_TYPE_TRANSFER_OUT)
	}

	st := &stats{}
	var send sender
	finish := func() {}
	target := *url
	if *direct {
		send, finish = kafkaSender(st, *linger)
		target = "Kafka"
	} else {
		send = httpSender(st, *url, *workers)
	}

	jobs := make(chan event, *workers)
	var wg sync.WaitGroup
	for range *workers {
		wg.Go(func() {
			for ev := range jobs {
				send(ev)
			}
		})
	}

	log.Printf("Sending %d events/s to %s for %s across %d accounts", *rate, target, *duration, *accounts)
	start := time.Now()
	ticker := time.NewTicker(tick)
	released := 0
loop:
	for now := range ticker.C {
		elapsed := now.Sub(start)
		if elapsed >= *duration {
			break loop
		}
		// Catch up on everything due so far, so a slow tick doesn't lower the rate.
		for due := int(elapsed.Seconds() * float64(*rate)); released < due; released++ {
			ev := event{
				ID:          uuid.NewString(),
				AccountID:   fmt.Sprintf("acct-%d", rand.IntN(*accounts)),
				Type:        types[rand.IntN(len(types))],
				AmountMinor: rand.Int64N(100_000) + 1,
			}
			select {
			case jobs <- ev:
			default:
				st.dropped.Add(1) // every worker is busy: the target can't keep up
			}
		}
	}
	ticker.Stop()
	close(jobs)
	wg.Wait()
	finish()
	elapsed := time.Since(start)

	slices.Sort(st.latencies)
	fmt.Printf("\nsent ok:      %d (%.0f events/s)\n", st.ok.Load(), float64(st.ok.Load())/elapsed.Seconds())
	fmt.Printf("failed:       %d\n", st.failed.Load())
	fmt.Printf("dropped:      %d (generator couldn't send on schedule)\n", st.dropped.Load())
	if len(st.latencies) > 0 {
		fmt.Printf("API latency:  p50=%s p95=%s p99=%s max=%s\n",
			pct(st.latencies, 0.50), pct(st.latencies, 0.95), pct(st.latencies, 0.99), st.latencies[len(st.latencies)-1])
	}
	if *record != "" {
		if err := os.WriteFile(*record, []byte(strings.Join(st.accepted, "\n")+"\n"), 0o644); err != nil {
			log.Fatalf("Failed to write %s: %v", *record, err)
		}
		fmt.Printf("recorded:     %d accepted event IDs in %s\n", len(st.accepted), *record)
	}
}

var apiTypes = map[fintechv1.TransactionType]string{
	fintechv1.TransactionType_TRANSACTION_TYPE_DEPOSIT:      "deposit",
	fintechv1.TransactionType_TRANSACTION_TYPE_TRANSFER_IN:  "transfer_in",
	fintechv1.TransactionType_TRANSACTION_TYPE_WITHDRAWAL:   "withdrawal",
	fintechv1.TransactionType_TRANSACTION_TYPE_TRANSFER_OUT: "transfer_out",
}

func httpSender(st *stats, url string, workers int) sender {
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: workers},
	}
	return func(ev event) {
		start := time.Now()
		if err := post(client, url, ev); err != nil {
			st.failed.Add(1)
			return
		}
		st.success(ev.ID, time.Since(start))
	}
}

func post(client *http.Client, url string, ev event) error {
	body, err := json.Marshal(map[string]any{
		"event_id":     ev.ID,
		"account_id":   ev.AccountID,
		"type":         apiTypes[ev.Type],
		"amount_minor": ev.AmountMinor,
		"currency":     "USD",
	})
	if err != nil {
		return err
	}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// kafkaSender produces events without waiting for each acknowledgement. The
// returned function flushes and waits for the outstanding results.
func kafkaSender(st *stats, lingerMs int) (sender, func()) {
	cfg := config.LoadKafka()
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.BootstrapServers,
		"enable.idempotence": true,
		"acks":               "all",
		"linger.ms":          lingerMs,
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for e := range producer.Events() {
			if m, ok := e.(*kafka.Message); ok {
				if m.TopicPartition.Error != nil {
					st.failed.Add(1)
					continue
				}
				st.success(m.Opaque.(string), 0)
			}
		}
	}()

	send := func(ev event) {
		value, err := proto.Marshal(&fintechv1.Transaction{
			EventId:     ev.ID,
			AccountId:   ev.AccountID,
			Type:        ev.Type,
			AmountMinor: ev.AmountMinor,
			Currency:    "USD",
			CreatedAt:   timestamppb.Now(),
		})
		if err == nil {
			err = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &cfg.Topic, Partition: kafka.PartitionAny},
				Key:            []byte(ev.AccountID),
				Value:          value,
				Opaque:         ev.ID,
			}, nil)
		}
		if err != nil {
			st.failed.Add(1)
		}
	}

	finish := func() {
		for producer.Flush(1000) > 0 {
		}
		producer.Close() // closes Events()
		<-done
	}
	return send, finish
}

func pct(sorted []time.Duration, p float64) time.Duration {
	return sorted[int(float64(len(sorted)-1)*p)].Round(100 * time.Microsecond)
}
