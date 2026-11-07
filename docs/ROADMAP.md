# Roadmap

Build a thin end-to-end pipeline early, then make it correct, then make it fast. Every phase ends with something runnable and demoable, and each has a **done-when** check before moving on.

- **Phases 1–3 are the core.** Stopping after Phase 3 still leaves a solid, correct event pipeline.
- **Phases 4 and 6 are independent** and can be done in either order.
- Keep the README's Status line and targets table current as phases are completed.

---

## Phase 1: Foundations

**Status:** ✅ Complete

**Goal:** The project builds, the local stack runs, and the event format is defined.

- [x] Add `go.mod` and `.gitignore`; fix compile errors; `go build ./...` and `go vet ./...` pass
- [x] Replace `docker-compose.yml` with Kafka in KRaft mode (no Zookeeper), Postgres, OTel Collector, Prometheus, Grafana, and Kafka UI
- [x] Define the `Transaction` event as a Protobuf message: `event_id`, `account_id`, `amount` (integer cents, never floats), `currency`, `type`, `created_at`
- [x] Create the `transactions` and `transactions.dlq` topics at startup (e.g. 6 partitions)
- [x] Add a `Makefile`: `make up`, `make down`, `make build`, `make test`, `make lint`
- [x] Load configuration from environment variables (no hard-coded `localhost:9092`)
- [x] Add GitHub Actions CI: build, vet, and test on every push

**Done when:** A fresh clone runs `make up && make build` without errors, and Kafka UI shows both topics.

**You'll learn:** Go modules, Kafka topics and partitions, Protobuf, Docker Compose.

---

## Phase 2: Core Pipeline + Observability

**Status:** ✅ Complete (baseline: ~650 events/s saturation; see README)

**Goal:** One event flows from API to database, and you can see it happen.

- [x] **Event API** (`cmd/api`): `POST /transactions` validates input, stamps `created_at`, and produces to Kafka keyed by `account_id`; check delivery reports
- [x] **Processor** (`cmd/processor`): consume, apply the event to an `accounts` balance table in Postgres, commit the offset after the write
- [x] Database migrations (e.g. `golang-migrate`) for `accounts` and `processed_events`
- [x] OpenTelemetry tracing on the API and processor, with trace context passed through Kafka headers so one trace covers API → Kafka → processor → DB
- [x] End-to-end latency histogram (`processed_at - created_at`) and a consumer lag metric
- [x] Grafana dashboard: throughput, latency (p50/p95/p99), and lag
- [x] **Baseline:** a single-threaded run at a fixed rate; record the numbers in the README

**Done when:** You `curl` an event, the balance changes in Postgres, and the full trace and latency appear in Grafana.

**You'll learn:** Kafka producers and consumer groups, `pgx`, OTel traces and metrics, context propagation.

---

## Phase 3: Correctness

**Status:** ✅ Complete (saturation ~1,700–2,000 events/s, up from ~650; chaos test: 0 lost, 0 double-applied)

**Goal:** The pipeline stays correct when things go wrong.

- [x] **Idempotency:** insert `event_id` into `processed_events` in the same DB transaction as the balance update; skip duplicates
- [x] **Ordering:** one goroutine per partition; document why this gives per-account ordering
- [x] **Retries:** a bounded number with backoff for transient errors (e.g. DB timeouts); invalid events go straight to the DLQ
- [x] **DLQ:** route failures to `transactions.dlq` with the error reason in headers; add a small replay tool
- [x] **Graceful shutdown:** on SIGTERM, finish in-flight messages, commit offsets, and close cleanly
- [x] **Business rules:** reject overdrafts, unknown currencies, etc.
- [x] **Tests:** unit tests for processing logic; integration tests with Testcontainers covering duplicate delivery, a crash before commit, and DLQ routing

**Done when:** Killing the processor mid-load and restarting it leaves balances exactly correct, with no double-applied or lost events, proven by a test.

**You'll learn:** At-least-once delivery, idempotency, transactional writes, Kafka rebalances, failure testing.

---

## Phase 4: Serving Layer

**Status:** ✅ Complete (push latency p50 ~10 ms at 1,000 events/s; NOTIFY has no measurable throughput cost)

**Goal:** Users see updates as they happen.

- [x] **Query API:** `GET /accounts/{id}` and `GET /accounts/{id}/transactions`
- [x] **Real-time push:** a WebSocket (or Server-Sent Events) endpoint that pushes balance changes to subscribed clients, via Postgres `LISTEN/NOTIFY` or an `account-updates` topic
- [x] Optional Redis cache for hot accounts, only if read latency calls for it (measured: not needed; reads p99 1.2 ms idle, 20 ms under write load)
- [x] A small demo page that shows a balance updating live
- [x] Extend the latency metric so it measures up to the push (SSE was chosen over WebSocket)

**Done when:** You send an event and watch the demo page update within the latency target.

**You'll learn:** WebSockets/SSE in Go, fan-out patterns, caching tradeoffs.

---

## Phase 5: Performance

**Goal:** Hit the targets, or understand exactly why you can't.

- [ ] Load generator: rewrite `loadtest/k6.js` against the Event API, plus xk6-kafka or a Go tool that produces directly to Kafka
- [ ] Ramp to 5,000 events/s, hold for 10 minutes, then step up to find the saturation point
- [ ] Profile with `pprof` and find the bottleneck (usually DB writes)
- [ ] Tune producer batching (`linger.ms`, `batch.size`, compression), partition count, DB connection pool, and batched DB writes per partition
- [ ] Record before/after numbers for each change in `docs/performance.md`
- [ ] Update the README targets table with measured results

**Done when:** There's a results table with real numbers, the saturation point, and what limits it.

**You'll learn:** Load testing, profiling, Kafka and Postgres tuning, reading latency percentiles.

---

## Phase 6: Workflows with Temporal

**Goal:** Use Temporal for something it's actually good at.

- [ ] Choose one multi-step flow, e.g. **settlement**: reserve funds → call an external payment provider (mocked) → confirm or reverse → notify
- [ ] The processor starts the workflow only for events that need it (e.g. `type = withdrawal`), not for every event
- [ ] Activities with retry policies and timeouts, plus a **compensation** step (refund on failure)
- [ ] Add Temporal to docker-compose; register the worker in `cmd/worker`
- [ ] Workflow tests with Temporal's test suite, including failure and compensation paths
- [ ] Confirm the Temporal path doesn't affect hot-path latency (compare against Phase 5 numbers)

**Done when:** A settlement survives a worker crash mid-flow and finishes (or compensates) correctly.

**You'll learn:** Durable execution, workflows vs. activities, the saga/compensation pattern.

---

## Phase 7: Deployment

**Goal:** Run it the way a production system runs.

- [ ] Multi-stage Dockerfiles for each service (small images, non-root user)
- [ ] Kubernetes manifests or a Helm chart: Deployments, Services, ConfigMaps, Secrets, health probes, resource limits
- [ ] Test locally on `kind` or `minikube` first
- [ ] Provision cloud infrastructure with Terraform: managed Kafka (MSK/Confluent), managed Postgres, a Kubernetes cluster
- [ ] Autoscale processors on consumer lag (KEDA), with replicas ≤ partition count
- [ ] Alerts on p99 latency, error rate, consumer lag, and DLQ growth
- [ ] Complete the README's production readiness checklist; deploy from CI

**Done when:** A load test against the cloud deployment meets the targets, and alerts fire when a failure is injected.

**You'll learn:** Kubernetes, Helm, Terraform, KEDA, operating a system in production.
