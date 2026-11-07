# Realtime FinTech Event Processing Platform

**Problem:** Fintech platforms often show users stale financial data (balances, transaction status) because events are processed in slow batches or through tightly coupled services. Slow updates hurt user experience and trust.

**Solution:** A real-time event processing platform built with Go and Kafka. Financial events are ingested through an API, streamed through Kafka partitioned by account, processed idempotently, and written to a state store that users can query or subscribe to, so updates show up in milliseconds instead of seconds.

**Technologies:** Go, Kafka, PostgreSQL, Temporal, OpenTelemetry, Prometheus / Grafana, Kubernetes

**Status:** 🚧 Phase 5 (Performance) complete: events flow from the Event API through Kafka to the Processor and into Postgres, and users see balance changes live: a Query API streams every committed change over Server-Sent Events, with a demo page at http://localhost:8082. Processing is ordered per account, idempotent, retried on transient errors, and dead-lettered on permanent ones, verified by integration and crash tests. See [Measured Performance](#measured-performance) for how it does against the targets below.

## Goals and Targets

The main metric is **end-to-end latency**: the time from when an event is created (timestamp set by the producer) to when its result is visible in the state store.

| Metric | Target | How to Measure |
| --- | --- | --- |
| Sustained throughput | 5,000 events/s (~430M events/day) | Load generator produce rate vs. processor commit rate |
| End-to-end latency | p50 < 20 ms, p99 < 50 ms | Histogram of `processed_at - created_at`, exported via OpenTelemetry |
| Ingestion API latency | p99 < 30 ms | k6 / OpenTelemetry HTTP server spans |
| Error rate | < 0.1% | Failed requests + events routed to the DLQ / total events |
| Consumer lag | < 1,000 messages at steady state | Kafka consumer group lag exported to Prometheus |
| Saturation point | Identify and document | Increase load step by step until p99 latency or lag stops being stable |

A baseline is measured first (a naive synchronous implementation) so that any improvement claimed is backed by numbers.

### Measured Performance

Everything runs locally in Docker on a laptop: one Processor instance, a 6-partition topic, and 1,000 accounts. End-to-end percentiles are estimated from histogram buckets, so they are approximate. The full investigation is in **[docs/performance.md](docs/performance.md)**.

**Phase 5: batched writes and tuning** (current)

| Metric | Target | Result |
| --- | --- | --- |
| Sustained throughput | 5,000 events/s | ✅ Held for 10 minutes |
| Saturation point | Identify | ~15,000–24,000 events/s, up from ~1,630 |
| Lost events in the 10-minute full-pipeline run | None | ✅ 3,292,347 accepted, 3,292,347 applied |
| End-to-end p50 | < 20 ms | ✅ 8.5 ms straight to Kafka; 40 ms with k6 and every service sharing the laptop |
| End-to-end p99 | < 50 ms | ❌ 406 ms straight to Kafka; 3.4 s with k6 and every service sharing the laptop |
| Event API p99 | < 30 ms | ❌ Over target under k6 at 5,000/s (p50 6.4 ms, p95 45 ms) |

**Phase 4: live push to users**

Measured on battery power, which slows the laptop, so the tail latencies are higher than in the Phase 3 runs. The `NOTIFY` comparisons here were capped by per-event round trips, which hid its real cost; see [docs/performance.md](docs/performance.md#other-things-tested).

| Measurement | Result |
| --- | --- |
| Latency to push at 1,000 events/s (created → change reaches the Query API), p50 / p95 / p99 | 10 / 88 / 361 ms. The same as commit latency, so `NOTIFY` and SSE add almost nothing |
| `GET /accounts/{id}` at 2,000 reads/s, p50 / p99 | 0.4 / 1.2 ms on its own; 1.0 / 20 ms while 1,000 writes/s are processed |

- **No Redis:** reads straight from Postgres already meet the p99 < 30 ms target, so a cache would only add invalidation problems. Worth revisiting if read traffic grows by orders of magnitude.

**Phase 3: one worker per partition**

| Load | Event API p50 / p99 | End-to-end p50 / p95 / p99 |
| --- | --- | --- |
| 1,000 events/s | 5.7 / 14 ms | 8 / 36 / 163 ms |
| 2,000 events/s | 5.7 / 27 ms | > 8 s (saturated) |
| 3,000 events/s | 5.6 / 34 ms | > 30 s (saturated) |

- **Saturation point:** about **1,700–2,000 events/s**, roughly **3× the baseline**. The six partitions are now processed in parallel.
- **Bottleneck:** still one Postgres transaction (four statements) per event. Phase 5 targets this with batched writes.

**Phase 2 baseline: one message at a time**

| Load | Event API p50 / p99 | End-to-end p50 / p95 / p99 | Max consumer lag |
| --- | --- | --- | --- |
| 100 events/s | 7.3 / 26 ms | 8 / 20 / 73 ms | 0 |
| 300 events/s | 6.4 / 14 ms | 8 / 19 / 83 ms | 5 |
| 1,000 events/s | 5.8 / 22 ms | > 5 s (saturated) | ~23,000 |

- **Saturation point:** about **650 events/s**. Beyond that, lag grows and latency is dominated by time spent waiting in Kafka.
- **Bottleneck:** sequential database round trips (BEGIN, two INSERTs, COMMIT) for every event. The Event API is not the limit.

## Architecture

```
                 ┌──────────────── OTel Collector ◀── traces / metrics from every service
                 │                       │
                 │                       ▼
                 │              Prometheus / Grafana (latency, throughput, consumer lag)
                 │
Event API ──▶ Kafka topic "transactions" ──▶ Processor (Go, consumer group)
(HTTP)        keyed by account_id             │  idempotent, ordered per partition, batched writes
                                              ├──▶ State store (Postgres) ──NOTIFY──▶ Query API ──SSE──▶ users (demo page)
                                              ├──▶ DLQ topic "transactions.dlq" (events that fail validation/processing)
                                              └──▶ Temporal (only for multi-step flows, e.g. settlement, refunds)
```

### Components

- **Event API (Go):** Accepts financial events over HTTP, validates them, stamps `created_at`, and produces them to Kafka with `account_id` as the message key.
- **Kafka:** A durable, partitioned event log. Keying by `account_id` sends all events for an account to the same partition, so they are processed in order.
- **Processor (Go):** A consumer group that applies business logic (balance updates, overdraft and currency checks). Each assigned partition gets its own worker goroutine, so events for one account are applied in order while partitions run in parallel. Under load, a worker applies everything queued for its partition in one database transaction. Offsets are committed only after an event is fully handled.
- **State store:** PostgreSQL as the source of truth. A trigger publishes every committed account change with `NOTIFY`, and a `version` column increases with each change. Redis turned out to be unnecessary (see [Measured Performance](#measured-performance)).
- **Query API (Go):** A separate read service that serves balances and paginated history, and streams balance changes to clients over Server-Sent Events. It listens to Postgres `NOTIFY` and fans each change out to the clients watching that account. Also serves a demo page.
- **Dead-letter queue (DLQ):** Events that can never succeed (malformed, invalid, or rejected by a business rule) go to a separate topic with the reason in the headers, so they never block the partition. A CLI lists them and replays them once the cause is fixed.
- **Temporal:** Orchestrates long-running, multi-step business processes started by specific events (e.g. settlement, refunds, chargebacks). It is deliberately kept **off** the per-event path to protect latency.
- **OpenTelemetry:** A cross-cutting concern, not a pipeline stage. Every service emits traces and metrics to an OTel Collector. Trace context travels in Kafka message headers, so each event's trace covers API → Kafka → Processor. Database work is shared by a batch, so it appears in the first event's trace and links to the others.

### Key Design Decisions

| Decision | Why |
| --- | --- |
| Partition by `account_id` | Guarantees per-account ordering without distributed locks |
| Idempotent processing (dedupe on `event_id`) | Kafka delivers at least once; reprocessing a duplicate must not apply it twice |
| Commit offsets after the state write | A crash causes a safe reprocess instead of data loss |
| One worker goroutine per partition | Per-account ordering with parallelism across partitions, and no shared mutable state |
| Retry transient errors, dead-letter permanent ones | A database outage must not flood the DLQ with valid events; a bad event must not block its partition |
| Short session timeout + optional static membership | A crashed processor's partitions are reassigned in ~10 s, or immediately when it restarts with the same `KAFKA_GROUP_INSTANCE_ID` |
| Separate read service (CQRS) | Reads and writes scale independently, and a burst of readers can't slow event processing |
| Server-Sent Events instead of WebSocket | Updates only flow server → client, so plain HTTP with built-in browser reconnection is enough; no extra library or protocol |
| Postgres `NOTIFY` from a trigger instead of an "account-updates" Kafka topic | `NOTIFY` is transactional: clients only hear about committed changes, and nothing can be published for a write that rolled back. Publishing to Kafka after the commit isn't atomic without an outbox. The tradeoff: a global lock at commit, which costs roughly 1.5–2× on p99 ([measured](docs/performance.md#other-things-tested)) and nothing at the median |
| Version number on every account | Clients drop any update older than what they've shown, so reconnects and snapshot/update races never move a balance backwards |
| Batched writes with no timer | Round trips and commits become per batch, which raised saturation about 10×; a batch is only what's already waiting, so light load adds no delay |
| Schema-defined events (Protobuf, linted with buf) | Producers and consumers share one definition of an event |
| Temporal only for multi-step workflows | Workflow starts add tens of ms; per-event consistency comes from the three items above |
| Instrument from day one | The latency targets can't be verified without end-to-end measurement |

### How the Processor Handles Failures

| Situation | What happens |
| --- | --- |
| Malformed or invalid event | Sent to the DLQ (`malformed` / `invalid`); processing continues |
| Business rule fails | Sent to the DLQ (`insufficient_funds` for a debit that would overdraw, `currency_mismatch`, `balance_overflow`); the balance is untouched |
| The database refuses a batch because of one event's contents | The batch is retried one event at a time, so only that event goes to the DLQ |
| Duplicate delivery | Skipped: `event_id` is already in `processed_events` |
| Postgres unavailable | Retried with exponential backoff (6 attempts, up to 5 s apart). If it still fails, the processor stops without committing that offset, and the event is retried after a restart |
| Processor killed (`kill -9`) | Events whose offsets weren't committed are redelivered and deduplicated. Partitions move to another instance after the 10 s session timeout, or immediately on restart with static membership |
| Graceful shutdown or rebalance | In-flight events finish, offsets are committed, and anything still buffered is left for the next owner of the partition |
| Processor paused for minutes (e.g. the machine slept) with static membership | When it wakes, the broker can treat its retried join as a second member with the same instance ID and fence it. The processor stops with a fatal error instead of continuing in an unknown state; an orchestrator must restart it |
| Query API loses its `LISTEN` connection | Reconnects with backoff, then sends every watched account a fresh snapshot, so changes made while disconnected aren't missed |
| A client reads the stream slowly | Only its latest pending update is kept: it skips intermediate balances instead of slowing other clients down |

## Project Structure

```
.
├── Makefile                     # make up / down / build / test / test-integration / lint / dlq
├── docker-compose.yml           # Kafka (KRaft), Postgres, OTel Collector, Jaeger, Prometheus, Grafana, Kafka UI
├── buf.yaml, buf.gen.yaml       # Protobuf lint and code generation config
├── proto/fintech/v1/            # Event schemas (source of truth)
├── migrations/                  # Postgres schema (golang-migrate)
├── deploy/                      # Config for the OTel Collector, Prometheus, Grafana (incl. dashboard)
├── docs/ROADMAP.md              # Phase-by-phase task checklist
├── loadtest/k6.js               # Load test script
├── .github/workflows/ci.yml     # Build, vet, unit + integration tests, lint on every push
└── src/
    ├── cmd/
    │   ├── api/                 # Event API: HTTP → validate → Kafka
    │   ├── processor/           # Processor entry point (logic lives in pkg/processor)
    │   ├── query/               # Query API entry point (logic lives in pkg/query)
    │   ├── dlq/                 # List and replay dead-lettered events
    │   └── loadgen/             # Fixed-rate load generator for the Event API
    ├── gen/fintech/v1/          # Generated Protobuf code (do not edit; run `make proto`)
    └── pkg/
        ├── config/              # Environment-based configuration
        ├── events/              # Event validation shared by the API and the Processor
        ├── processor/           # Partition workers, retries, DLQ, Postgres store, integration tests
        ├── query/               # Read API, SSE stream, NOTIFY listener, demo page, integration tests
        ├── workflow/temporal.go # Temporal worker setup
        └── telemetry/otel.go    # OpenTelemetry setup
```

## Phased Plan

See [docs/ROADMAP.md](docs/ROADMAP.md) for the detailed task checklist and done-when criteria for each phase.

1. **Phase 1 – Foundations:** Add `go.mod`, define the event schema, and modernize the local stack (Kafka in KRaft mode, Postgres, OTel Collector, Prometheus, Grafana) in `docker-compose.yml`.
2. **Phase 2 – Core pipeline + observability:** Event API → Kafka → Processor → Postgres, with OpenTelemetry tracing and the end-to-end latency histogram in place from the start. Measure a baseline.
3. **Phase 3 – Correctness:** Idempotency, offset commit strategy, retries, DLQ, and graceful shutdown. Add unit and integration tests.
4. **Phase 4 – Serving layer:** Query API and WebSocket push. Add Redis caching if read latency calls for it.
5. **Phase 5 – Performance:** Load test against the targets, find the saturation point, and tune (partitions, batching, consumer concurrency, connection pools).
6. **Phase 6 – Workflows:** Add Temporal for one real multi-step flow (e.g. settlement).
7. **Phase 7 – Deployment:** Kubernetes manifests / Helm, managed Kafka, and a production readiness checklist.

## Local Environment

**Prerequisites:** Go (see `go.mod` for the version), Docker, [buf](https://buf.build/docs/installation), `protoc-gen-go`, and `golangci-lint`.

1. **Start the stack, create the topics, and run migrations:**
   ```bash
   make up
   ```
2. **Run the Event API, the Processor, and the Query API** (in separate terminals):
   ```bash
   make run-api
   make run-processor
   make run-query
   ```
   Then open the demo page at **http://localhost:8082**: deposits and withdrawals show up live, with the time from click to update.
3. **Send a transaction:**
   ```bash
   curl -X POST localhost:8081/transactions \
     -d '{"account_id":"acct-1","type":"deposit","amount_minor":5000,"currency":"USD"}'
   # {"event_id":"..."}  (202 Accepted: stored in Kafka, applied shortly after)
   ```
   `type` is one of `deposit`, `withdrawal`, `transfer_in`, `transfer_out`. Amounts are in minor units (cents). Send an `event_id` (UUID) to make retries safe: the Processor applies each ID only once.
4. **Read the balance and history, or watch it live:**
   ```bash
   curl localhost:8082/accounts/acct-1
   curl "localhost:8082/accounts/acct-1/transactions?limit=10"   # newest first; pass next_cursor to page
   curl -N localhost:8082/accounts/acct-1/events                 # Server-Sent Events: snapshot, then each change
   ```
5. **Generate load:**
   ```bash
   make loadgen ARGS="-rate 200 -duration 60s"
   # -debits adds withdrawals (some get rejected for insufficient funds)
   # -record ids.txt saves accepted event IDs so you can check none were lost
   ```
6. **Inspect and replay the DLQ:**
   ```bash
   make dlq          # list dead-lettered events with their reason
   make dlq-replay   # republish them to the main topic once the cause is fixed
   ```
7. **Explore:**

   | Service | URL |
   | --- | --- |
   | Grafana: **Fintech → Transaction Pipeline** dashboard (latency, throughput, lag) | http://localhost:3000 |
   | Jaeger: end-to-end traces (API → Kafka → Processor → Postgres) | http://localhost:16686 |
   | Kafka UI (topics, messages, consumer groups) | http://localhost:8080 |
   | Prometheus | http://localhost:9090 |
   | Demo page and Query API | http://localhost:8082 |
   | Event API | http://localhost:8081 |
   | Kafka (from the host) | `localhost:9092` |
   | Postgres | `localhost:5432` (user/password/db: `fintech`) |
   | OTLP endpoint | `localhost:4317` (gRPC), `localhost:4318` (HTTP) |

8. **Stop:** `make down` (use `docker compose down -v` to also delete the Postgres data).

**Configuration** is read from environment variables:

| Variable | Default |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `KAFKA_TOPIC` | `transactions` |
| `KAFKA_DLQ_TOPIC` | `transactions.dlq` |
| `KAFKA_GROUP_ID` | `transaction-processor` |
| `KAFKA_GROUP_INSTANCE_ID` | unset. Set a unique, stable ID per Processor instance for static membership |
| `DATABASE_URL` | `postgres://fintech:fintech@localhost:5432/fintech?sslmode=disable` |
| `HTTP_ADDR` (Event API) | `:8081` |
| `CORS_ALLOWED_ORIGINS` (Event API) | `http://localhost:8082`, comma-separated |
| `QUERY_HTTP_ADDR` (Query API) | `:8082` |
| `EVENT_API_URL` (Query API, used by the demo page) | `http://localhost:8081` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` |

**Changing the event schema:** edit `proto/fintech/v1/*.proto`, then run `make proto` and commit the regenerated code.

## Cloud Deployment

1. **Infrastructure:** Provision managed Kafka (e.g. Amazon MSK or Confluent Cloud), managed Postgres, and a Kubernetes cluster with Terraform.
2. **Configuration:** Supply `bootstrap.servers`, credentials (SASL/TLS), and database URLs through environment variables / Kubernetes Secrets. Never hard-code them.
3. **Production readiness checklist:**
   - Topics created with the right partition count and replication factor ≥ 3
   - Credentials and least-privilege ACLs verified
   - Liveness/readiness probes and graceful shutdown in place
   - Dashboards and alerts for latency, error rate, and consumer lag
   - Integration tests passing against the deployed environment

## Load Testing

An event pipeline is tested by **event rate**, not by concurrent users.

1. **Tools:** `k6` for the Event API (HTTP), and the [xk6-kafka](https://github.com/mostafa/xk6-kafka) extension or a Go load generator to produce directly to Kafka.
2. **Scenario:**
   - Ramp from 0 to 5,000 events/s over 2 minutes.
   - Hold for 10 minutes.
   - Step up in increments to find the saturation point.
3. **Run:**
   ```bash
   k6 run loadtest/k6.js
   ```
4. **Observe:** Watch end-to-end latency percentiles, throughput, error rate, and consumer lag in Grafana.

## Testing

```bash
make test               # unit tests, no Docker needed
make test-integration   # starts real Kafka and Postgres with Testcontainers (needs Docker)
```

**Unit tests** cover request and event validation, CORS, error classification (transient vs. permanent), retry/backoff behavior, trace propagation through Kafka headers, the live-update hub (fan-out, latest-wins), and pagination cursors.

**Query API integration tests** ([src/pkg/query/integration_test.go](src/pkg/query/integration_test.go)) run against real Postgres with the migrations and triggers applied. They cover the stream sending a snapshot then live changes, a new account's stream starting with `not_found`, no leaking of other accounts' updates, the account endpoint, and keyset pagination across pages.

**Processor integration tests** ([src/pkg/processor/integration_test.go](src/pkg/processor/integration_test.go)) run the real Processor against real Kafka and Postgres:

| Test | Proves |
| --- | --- |
| `TestAppliesEventsInOrderPerAccount` | Per-account ordering holds while partitions run concurrently (an out-of-order withdrawal would overdraw) |
| `TestDuplicateDeliveryIsAppliedOnce` | The same event delivered three times changes the balance once |
| `TestCrashBetweenWriteAndOffsetCommit` | A crash after the DB write but before the offset commit doesn't double-apply after restart |
| `TestFullReplayIsIdempotent` | Reprocessing the entire topic from the beginning leaves balances unchanged |
| `TestInvalidEventsGoToDLQ` | Malformed, invalid, overdraft, and currency-mismatch events land in the DLQ with the right reason, and valid events keep flowing |
| `TestCreditsAreAcceptedIntoNegativeAccount` | Only debits can be refused for insufficient funds (a regression found by the chaos test) |
| `TestGracefulShutdownCommitsOffsets` | After a clean stop, a restarted processor doesn't see already-handled events |

Breaking deduplication on purpose makes the duplicate, crash, and replay tests fail, so they do catch the bugs they're meant to catch.

**Chaos test (manual):** with `make loadgen` sending 1,500 events/s and `-record` saving every accepted ID, the Processor was killed with `kill -9` twice and restarted. All 59,640 accepted events were applied exactly once (none missing, and the total balance change matched the sum of the events), and the restarted processor got its partitions back within 0.1 s using static membership.

## Technical Challenges

### Ordering and Concurrency

**Why it's hard:** Processing many events in parallel can apply events for the same account out of order or cause race conditions.

**How it's solved:** Partitioning by `account_id` plus a single owner per partition gives per-account ordering and parallelism across accounts, without locks or shared mutable state.

### Correctness Under Failure

**Why it's hard:** Kafka delivers at least once. Crashes, rebalances, and retries can cause the same event to be processed twice or lost.

**How it's solved:** Idempotent writes keyed by `event_id`, committing offsets only after an event is fully handled, bounded retries for transient errors, and a DLQ for events that can never succeed. See [How the Processor Handles Failures](#how-the-processor-handles-failures).

### Observability

**Why it's hard:** In an asynchronous pipeline, a slow or failing event is hard to trace across services.

**How it's solved:** OpenTelemetry trace context passed through Kafka headers, an end-to-end latency histogram, and consumer lag metrics with alerting.

## Learning Goals

- Build an event-driven pipeline in Go with Kafka (partitioning, consumer groups, delivery semantics)
- Design for correctness: idempotency, ordering, retries, and dead-letter queues
- Instrument a distributed system end to end with OpenTelemetry
- Measure and tune performance against explicit latency and throughput targets
- Use Temporal where it fits best: long-running, multi-step workflows

## Learning Resources

- **Go:** [A Tour of Go](https://go.dev/tour/), [Effective Go](https://go.dev/doc/effective_go), [Go by Example](https://gobyexample.com/)
- **Kafka:** [Kafka documentation](https://kafka.apache.org/documentation/), [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)
- **Temporal:** [Temporal Go SDK docs](https://docs.temporal.io/develop/go)
- **OpenTelemetry:** [OpenTelemetry Go docs](https://opentelemetry.io/docs/languages/go/)
