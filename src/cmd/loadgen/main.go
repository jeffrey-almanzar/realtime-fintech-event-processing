// Load generator: sends transactions to the Event API at a fixed rate and
// reports the client-side results. End-to-end latency is measured by the
// processor and shown in Grafana.
//
//	go run ./src/cmd/loadgen -rate 200 -duration 60s

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	url := flag.String("url", "http://localhost:8081/transactions", "Event API endpoint")
	rate := flag.Int("rate", 100, "requests per second")
	duration := flag.Duration("duration", 30*time.Second, "how long to send")
	accounts := flag.Int("accounts", 1000, "number of distinct account IDs")
	workers := flag.Int("workers", 64, "concurrent HTTP clients")
	flag.Parse()

	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *workers},
	}

	var (
		ok, failed, dropped atomic.Int64
		mu                  sync.Mutex
		latencies           []time.Duration
	)

	jobs := make(chan struct{}, *workers)
	var wg sync.WaitGroup
	for range *workers {
		wg.Go(func() {
			for range jobs {
				start := time.Now()
				if err := send(client, *url, *accounts); err != nil {
					failed.Add(1)
					continue
				}
				elapsed := time.Since(start)
				ok.Add(1)
				mu.Lock()
				latencies = append(latencies, elapsed)
				mu.Unlock()
			}
		})
	}

	log.Printf("Sending %d req/s to %s for %s across %d accounts", *rate, *url, *duration, *accounts)
	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	deadline := time.After(*duration)
	start := time.Now()
loop:
	for {
		select {
		case <-ticker.C:
			select {
			case jobs <- struct{}{}:
			default:
				dropped.Add(1) // every worker is busy: the API can't keep up
			}
		case <-deadline:
			break loop
		}
	}
	ticker.Stop()
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)

	slices.Sort(latencies)
	fmt.Printf("\nsent ok:      %d (%.0f req/s)\n", ok.Load(), float64(ok.Load())/elapsed.Seconds())
	fmt.Printf("failed:       %d\n", failed.Load())
	fmt.Printf("dropped:      %d (generator couldn't send on schedule)\n", dropped.Load())
	if len(latencies) > 0 {
		fmt.Printf("API latency:  p50=%s p95=%s p99=%s max=%s\n",
			pct(latencies, 0.50), pct(latencies, 0.95), pct(latencies, 0.99), latencies[len(latencies)-1])
	}
}

var types = []string{"deposit", "deposit", "deposit", "withdrawal", "transfer_in", "transfer_out"}

func send(client *http.Client, url string, accounts int) error {
	body, err := json.Marshal(map[string]any{
		"account_id":   fmt.Sprintf("acct-%d", rand.IntN(accounts)),
		"type":         types[rand.IntN(len(types))],
		"amount_minor": rand.Int64N(100_000) + 1,
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

func pct(sorted []time.Duration, p float64) time.Duration {
	return sorted[int(float64(len(sorted)-1)*p)].Round(100 * time.Microsecond)
}
