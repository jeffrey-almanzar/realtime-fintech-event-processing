# Realtime FinTech Event Processing Platform

**Problem:** Fintech platforms often show users stale financial data (balances, transaction status) because events are processed in slow batches or through tightly coupled services. Slow updates hurt user experience and trust.

**Solution:** A real-time event processing platform built with Go and Kafka. Financial events are ingested through an API, streamed through Kafka partitioned by account, processed idempotently, and written to a state store that users can query or subscribe to, so updates show up in milliseconds instead of seconds.

**Technologies:** Go, Kafka, PostgreSQL / Redis, Temporal, OpenTelemetry, Prometheus / Grafana, Kubernetes

**Status:** 🚧 Phase 1 (Foundations) complete: the local stack runs, events are defined in Protobuf, and a producer/consumer exchange them through Kafka. The targets below are goals, not measured results yet.

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

## Architecture

```
                 ┌──────────────── OTel Collector ◀── traces / metrics from every service
                 │                       │
                 │                       ▼
                 │              Prometheus / Grafana (latency, throughput, consumer lag)
                 │
Event API ──▶ Kafka topic "transactions" ──▶ Processor (Go, consumer group)
(HTTP/gRPC)   keyed by account_id             │  idempotent, ordered per partition
                                              ├──▶ State store (Postgres / Redis) ──▶ Query API / WebSocket ──▶ users
                                              ├──▶ DLQ topic "transactions.dlq" (events that fail validation/processing)
                                              └──▶ Temporal (only for multi-step flows, e.g. settlement, refunds)
```

### Components

- **Event API (Go):** Accepts financial events over HTTP/gRPC, validates them against the schema, stamps `created_at`, and produces them to Kafka with `account_id` as the message key.
- **Kafka:** A durable, partitioned event log. Keying by `account_id` sends all events for an account to the same partition, so they are processed in order.
- **Processor (Go):** A consumer group that applies business logic (balance updates, status changes). Each partition is handled by a single goroutine, so it needs no shared mutable state. It commits offsets only after the state write succeeds.
- **State store:** PostgreSQL as the source of truth, with Redis as an optional cache for fast reads.
- **Query API / WebSocket:** Serves current state to users and pushes updates as they happen.
- **Dead-letter queue (DLQ):** Events that fail after retries go to a separate topic for inspection and replay, so they never block the partition.
- **Temporal:** Orchestrates long-running, multi-step business processes started by specific events (e.g. settlement, refunds, chargebacks). It is deliberately kept **off** the per-event path to protect latency.
- **OpenTelemetry:** A cross-cutting concern, not a pipeline stage. Every service emits traces and metrics to an OTel Collector. Trace context travels in Kafka message headers, so a single trace covers API → Kafka → processor → state store.

### Key Design Decisions

| Decision | Why |
| --- | --- |
| Partition by `account_id` | Guarantees per-account ordering without distributed locks |
| Idempotent processing (dedupe on `event_id`) | Kafka delivers at least once; reprocessing a duplicate must not apply it twice |
| Commit offsets after the state write | A crash causes a safe reprocess instead of data loss |
| Schema-defined events (Protobuf + schema registry) | Producers and consumers can evolve without breaking each other |
| Temporal only for multi-step workflows | Workflow starts add tens of ms; per-event consistency comes from the three items above |
| Instrument from day one | The latency targets can't be verified without end-to-end measurement |

## Project Structure

```
.
├── Makefile                     # make up / down / build / test / lint / proto
├── docker-compose.yml           # Kafka (KRaft), Postgres, OTel Collector, Prometheus, Grafana, Kafka UI
├── buf.yaml, buf.gen.yaml       # Protobuf lint and code generation config
├── proto/fintech/v1/            # Event schemas (source of truth)
├── deploy/                      # Config for the OTel Collector, Prometheus, Grafana
├── docs/ROADMAP.md              # Phase-by-phase task checklist
├── loadtest/k6.js               # Load test script
├── .github/workflows/ci.yml     # Build, vet, test, lint on every push
└── src/
    ├── cmd/
    │   ├── producer/main.go     # Kafka producer (to become the Event API)
    │   └── consumer/main.go     # Kafka consumer (to become the Processor)
    ├── gen/fintech/v1/          # Generated Protobuf code (do not edit; run `make proto`)
    └── pkg/
        ├── config/              # Environment-based configuration
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

1. **Start the stack and create the topics:**
   ```bash
   make up
   ```
2. **Run the consumer and producer** (in separate terminals):
   ```bash
   make run-consumer
   make run-producer
   ```
   The producer sends 10 sample `Transaction` events; the consumer logs each one with its latency.
3. **Explore:**

   | Service | URL |
   | --- | --- |
   | Kafka UI (topics, messages, consumer groups) | http://localhost:8080 |
   | Grafana | http://localhost:3000 |
   | Prometheus | http://localhost:9090 |
   | Kafka (from the host) | `localhost:9092` |
   | Postgres | `localhost:5432` (user/password/db: `fintech`) |
   | OTLP endpoint | `localhost:4317` (gRPC), `localhost:4318` (HTTP) |

4. **Stop:** `make down` (use `docker compose down -v` to also delete the Postgres data).

**Configuration** is read from environment variables:

| Variable | Default |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `KAFKA_TOPIC` | `transactions` |
| `KAFKA_DLQ_TOPIC` | `transactions.dlq` |
| `KAFKA_GROUP_ID` | `transaction-processor` |

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

1. **Unit tests:** Test business logic as pure functions using Go's `testing` package, and Temporal workflows with the Temporal test suite.
   ```go
   func TestApplyTransaction(t *testing.T) {
       // Given an account state and an event, assert the resulting state
   }
   ```
2. **Integration tests:** Use [Testcontainers](https://golang.testcontainers.org/) to start Kafka and Postgres, then verify the end-to-end flow, including duplicate delivery and DLQ routing.

## Technical Challenges

### Ordering and Concurrency

**Why it's hard:** Processing many events in parallel can apply events for the same account out of order or cause race conditions.

**How it's solved:** Partitioning by `account_id` plus a single owner per partition gives per-account ordering and parallelism across accounts, without locks or shared mutable state.

### Correctness Under Failure

**Why it's hard:** Kafka delivers at least once. Crashes, rebalances, and retries can cause the same event to be processed twice or lost.

**How it's solved:** Idempotent writes keyed by `event_id`, committing offsets only after the state write succeeds, bounded retries, and a DLQ for events that keep failing.

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
