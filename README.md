# Realtime FinTech Event Processing Platform

[![CI](https://github.com/jeffrey-almanzar/realtime-fintech-event-processing/actions/workflows/ci.yml/badge.svg)](https://github.com/jeffrey-almanzar/realtime-fintech-event-processing/actions/workflows/ci.yml)

An event-driven payments backend in Go: transactions flow through Kafka into Postgres, balances update live in the browser, and withdrawals settle through durable Temporal workflows that refund the customer when a payment fails. It keeps its balances correct when processes are killed under load, and there are tests to prove it.

![Demo: a deposit, a withdrawal that settles, and a declined withdrawal that is refunded automatically](docs/images/demo.gif)

*A deposit, a withdrawal that settles, and a withdrawal the payment provider declines, which the workflow refunds automatically. Every balance change is pushed to the browser as it commits.*

**Problem:** Financial apps often show stale balances because events are processed in slow batches or through tightly coupled services. Keeping them fast is easy; keeping them fast *and correct* through crashes, retries, and duplicate deliveries is the hard part.

**Solution:** Events are streamed through Kafka partitioned by account, applied idempotently in batched database transactions, and pushed to clients the moment they commit. Multi-step processes that involve outside systems run as Temporal workflows, kept off the main event path.

**Technologies:** Go, Kafka, PostgreSQL, Temporal, OpenTelemetry, Prometheus, Grafana, Jaeger, Docker, Kubernetes, Protobuf, k6

## Try It

All you need is Docker. Every service is built and started for you, which takes a few minutes the first time:

```bash
git clone https://github.com/jeffrey-almanzar/realtime-fintech-event-processing.git
cd realtime-fintech-event-processing
make demo
```

| Open | To see |
| --- | --- |
| [localhost:8082](http://localhost:8082) | **The demo page.** Deposit, then withdraw an amount ending in `.13` to watch a decline and automatic refund |
| [localhost:3000](http://localhost:3000) | **Grafana:** Fintech → Transaction Pipeline. Run `make demo-load` to fill the charts |
| [localhost:16686](http://localhost:16686) | **Jaeger:** a single event's path across services |
| [localhost:8233](http://localhost:8233) | **Temporal:** every settlement workflow and its history |
| [localhost:8080](http://localhost:8080) | **Kafka UI:** topics, messages, and consumer groups |

`make smoke` tests the running system end to end, and `make demo-down` stops it and deletes its data.

## Results

Measured on one laptop, with the broker, database, services, and load generator all sharing it. The full investigation is in **[docs/performance.md](docs/performance.md)**.

| Metric | Target | Result |
| --- | --- | --- |
| Sustained throughput | 5,000 events/s | ✅ Held for 10 minutes |
| Saturation point | Identify it | ~15,000–24,000 events/s, **about 10× the ~1,630 before optimizing** |
| Events lost in the 10-minute full-pipeline run | None | ✅ 3,292,347 accepted, 3,292,347 applied |
| Events lost when killing processes under load | None | ✅ None, with `kill -9` and with force-deleted Kubernetes pods |
| End-to-end p50 | < 20 ms | ✅ 8.5 ms straight to Kafka; 40 ms with k6 and every service on the laptop |
| End-to-end p99 | < 50 ms | ❌ **Missed:** 406 ms straight to Kafka; 3.4 s with k6 and every service on the laptop |
| Event API p99 | < 30 ms | ❌ **Missed** under k6 at 5,000/s (p50 6.4 ms, p95 45 ms) |

The p99 misses are documented rather than tuned away: most of the seconds-long tail came from sharing the laptop, and what remains points at slow commits during Postgres checkpoints.

## Architecture

```
                         ┌── OTel Collector ◀── traces and metrics from every service
                         │        └──▶ Prometheus / Grafana, Jaeger
                         │
Event API ──▶ Kafka "transactions" ──▶ Processor (consumer group, one worker per partition)
 (HTTP)       keyed by account_id        │  ordered per account, idempotent, batched writes
    ▲                                    │
    │                                    ├──▶ Postgres ──NOTIFY──▶ Query API ──SSE──▶ browser
    │                                    │
    │                                    ├──▶ Kafka "transactions.dlq" (events that can never succeed)
    │                                    │
    │                                    └──▶ settlements outbox table (same transaction as the debit)
    │                                              │
    │                                              ▼
    │                                    Settlement worker ──▶ Temporal ──▶ mock payment provider
    │                                              │
    └── refund events go back into Kafka ◀─────────┘  (when a charge fails for good)
```

| Component | What it does |
| --- | --- |
| **Event API** | Validates transactions and publishes them to Kafka keyed by account, responding once the broker has stored them |
| **Processor** | Applies events to balances, with overdraft and currency checks. One worker per partition keeps each account's events in order, and everything queued is applied in one transaction |
| **Query API** | Separate read service: balances, paginated history, and a Server-Sent Events stream of changes, fed by Postgres `NOTIFY`. Serves the demo page |
| **Settlement worker** | Starts workflows from the outbox and runs them: charge the provider, and refund if that fails for good |
| **DLQ tool** | `make dlq` lists rejected events with their reasons, and `make dlq-replay` republishes them once the cause is fixed |

## Design Decisions

| Decision | Why |
| --- | --- |
| Partition by `account_id`, one worker per partition | Per-account ordering and parallelism across accounts, with no distributed locks or shared mutable state |
| Idempotent processing (dedupe on `event_id`) | Kafka delivers at least once, so a redelivered event must not apply twice |
| Commit offsets only after the database commit | A crash causes a safe reprocess, never a lost event |
| Batched writes with no timer | Round trips and fsyncs become per batch, which is where the 10× came from. A batch is only what's already waiting, so light load adds no delay |
| Retry transient errors, dead-letter permanent ones | A database outage must not flood the DLQ with valid events, and a bad event must not block its partition |
| Publish dead letters before the commit | A crash redelivers the batch and decides it identically; publishing afterward could re-decide a rejection against later balances |
| Transactional outbox between the Processor and Temporal | A debit can't commit without its settlement, and a slow Temporal delays settlements rather than event processing |
| Refunds are ordinary events with an ID derived from the withdrawal | They get the same ordering and live updates as everything else, and deduplication makes retries safe |
| Postgres `NOTIFY` instead of a Kafka updates topic | It's transactional, so clients only hear about committed changes. The tradeoff is a global commit lock, which [costs roughly 1.5–2× on p99](docs/performance.md#other-things-tested) |
| Server-Sent Events instead of WebSocket | Updates only flow one way, so plain HTTP with built-in reconnection is enough |
| A version number on every account | Clients drop anything older than what they've shown, so a reconnect can never move a balance backward |
| Separate read service (CQRS) | Reads and writes scale independently, and readers can't slow event processing |
| No Redis | Reads straight from Postgres took 1.2 ms at p99 on their own and 20 ms under write load, so a cache would only add invalidation problems |
| Integer minor units for money | Floating point can't represent cents exactly |

## How It Handles Failure

| Situation | What happens |
| --- | --- |
| Duplicate delivery | Skipped: `event_id` is already recorded |
| Malformed or invalid event | Sent to the DLQ with the reason; processing continues |
| Debit that would overdraw, or wrong currency | Sent to the DLQ; the balance is untouched |
| One event makes the database refuse a batch | The batch is retried one event at a time, so only that event goes to the DLQ |
| Postgres unavailable | Retried with exponential backoff, then the Processor stops without committing that offset and retries after a restart |
| Processor killed | Uncommitted events are redelivered and deduplicated. Partitions move to another instance within the 10 s session timeout, or immediately on restart with static membership (`KAFKA_GROUP_INSTANCE_ID`) |
| Graceful shutdown or rebalance | In-flight events finish and offsets are committed before the partition moves |
| Settlement worker killed mid-workflow | Temporal resumes from the last completed step on restart |
| Settlement worker or Temporal down | Withdrawals still debit and events keep flowing. Settlements wait in the outbox |
| Provider declines, or is unavailable for 2 minutes | The workflow refunds the account |
| Query API loses its `LISTEN` connection | Reconnects, then sends every watched account a fresh snapshot |
| A client reads the stream slowly | It skips intermediate balances instead of slowing other clients |

Temporal's timeline makes this visible. Here the provider was unavailable through 10 attempts over 2 minutes, so the workflow gave up, published the refund, and recorded the outcome:

![Temporal timeline: ChargeProvider fails 10 times, then PublishRefund and RecordOutcome complete](docs/images/temporal.png)

## What Testing Caught

Most of these only appeared because of testing against real infrastructure, or because a surprising measurement got investigated instead of explained away.

| Found by | Problem | Fix |
| --- | --- | --- |
| Chaos test | Deposits were rejected as "insufficient funds" on accounts that were already negative | Only debits can be refused for lack of funds; regression test added |
| Chaos test | After `kill -9`, nothing processed for 75 s while Kafka waited for the dead consumer | Shorter session timeout and optional static membership: **75 s → 0.1 s** |
| CI | A worker failing during shutdown could make the Processor exit with status 0, hiding the failure from an orchestrator | Check for worker errors after all workers stop. It only reproduced on CI's runners |
| Code review of batching | A rejected first event set a new account's currency, so the next valid event was refused | The currency now comes from the first *applied* event; regression test added |
| Profiling | An earlier conclusion that `NOTIFY` had no measurable cost was wrong: per-event round trips had been hiding its global commit lock | Corrected, and [measured properly](docs/performance.md#other-things-tested) |
| 10-minute test | The 60 s runs looked far better than the system really is, because most contained no checkpoint | Results now report the sustained numbers, and the p99 target is marked as missed |
| Fresh-clone test | `make demo-load` failed because of a Compose dependency across profiles | Fixed, and it now runs in CI |

## Observability

Every service exports traces and metrics through an OTel Collector. Trace context travels in Kafka headers, so one trace follows an event across services. This one took 16.8 ms from the API request to the database commit:

![Jaeger trace of one event from the Event API through Kafka to the Processor and Postgres](docs/images/trace.png)

The Grafana dashboard covers end-to-end latency, throughput, consumer lag, DLQ rate, push latency, latency per stage, and settlements. This capture is at 400 events/s:

![Grafana dashboard showing latency, throughput, consumer lag, and API request rate](docs/images/grafana.png)

## Testing

```bash
make test               # unit tests, no Docker needed
make test-integration   # real Kafka and Postgres with Testcontainers
make smoke              # end-to-end against a running system
```

**CI** runs on every push, with five jobs:
- build, vet, unit tests with the race detector, and lint
- integration tests
- Protobuf lint
- `make demo` from a clean checkout, then the smoke test and load generator
- deploying to a real Kubernetes cluster, then the smoke test

**Processor integration tests** run the real Processor against real Kafka and Postgres, including:
- ordering across concurrent partitions
- duplicate delivery
- a crash between the database write and the offset commit
- replaying the whole topic
- DLQ routing, and one bad event inside a batch
- graceful shutdown

Breaking deduplication on purpose made the duplicate, crash, and replay tests fail, so they do catch what they're meant to catch.

**Workflow tests** run the real workflow and activity code in Temporal's test environment: success, a decline that isn't retried, retries that eventually succeed, a provider down past the deadline, and a refund that keeps retrying until it succeeds.

**Chaos tests (run by hand):**

| Scenario | Result |
| --- | --- |
| `kill -9` the Processor twice at 1,500 events/s | All 59,640 accepted events applied exactly once |
| `kill -9` the settlement worker during a charge | The workflow finished after restart, and a refund applied exactly once |
| On Kubernetes: force-delete Processor and worker pods and scale up under load | All 11,997 accepted events applied, and all 1,001 account balances matched their ledgers |

## Running It

**Everything in Docker:** `make demo`, as above.

**On Kubernetes** (needs Docker, [kind](https://kind.sigs.k8s.io/), and `kubectl`):

```bash
make k8s-up        # create a local cluster, build and load images, deploy
make k8s-smoke     # run the smoke test against the cluster
make k8s-forward   # then open http://localhost:8082
make k8s-down      # delete the cluster
```

The Processor runs as 2 replicas sharing the topic's partitions: `kubectl -n fintech scale deployment/processor --replicas=3` rebalances them live. The manifests are in [deploy/k8s](deploy/k8s).

**From source, for development** (needs Go; see `go.mod` for the version):

```bash
make up              # backing services, topics, and migrations
make run-api         # each of these in its own terminal
make run-processor
make run-query
make run-worker
```

To change the event schema, install [buf](https://buf.build/docs/installation) and `protoc-gen-go`, edit `proto/fintech/v1/`, and run `make proto`. `make lint` also needs `golangci-lint`.

**API:**

```bash
curl -X POST localhost:8081/transactions \
  -d '{"account_id":"acct-1","type":"deposit","amount_minor":5000,"currency":"USD"}'
# 202 {"event_id":"..."}: stored in Kafka, applied shortly after

curl localhost:8082/accounts/acct-1                               # balance
curl "localhost:8082/accounts/acct-1/transactions?limit=10"       # history, newest first
curl -N localhost:8082/accounts/acct-1/events                     # live updates
```

`type` is one of `deposit`, `withdrawal`, `transfer_in`, or `transfer_out`. Send an `event_id` (UUID) to make retries safe. The mock provider picks its behavior from an amount's last two digits: `13` is declined, `99` fails twice and then succeeds, and `98` stays unavailable until the workflow refunds after 2 minutes.

**Load testing:**

```bash
make demo-load                                         # 60 s of traffic through the demo
make loadgen ARGS="-direct -rate 5000 -duration 60s"   # straight to Kafka, skipping HTTP
k6 run -e TARGET=5000 -e HOLD=10m loadtest/k6.js       # ramp and hold against the Event API
```

<details>
<summary><strong>Configuration</strong> (environment variables)</summary>

| Variable | Default |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `KAFKA_TOPIC` / `KAFKA_DLQ_TOPIC` | `transactions` / `transactions.dlq` |
| `KAFKA_GROUP_ID` | `transaction-processor` |
| `KAFKA_GROUP_INSTANCE_ID` | Unset. Set a unique, stable ID per Processor for static membership |
| `DATABASE_URL` | `postgres://fintech:fintech@localhost:5432/fintech?sslmode=disable` |
| `PROCESSOR_MAX_BATCH` | `500` |
| `HTTP_ADDR` (Event API) | `:8081` |
| `CORS_ALLOWED_ORIGINS` (Event API) | `http://localhost:8082` |
| `QUERY_HTTP_ADDR` (Query API) | `:8082` |
| `EVENT_API_URL` (demo page) | `http://localhost:8081` |
| `TEMPORAL_ADDRESS` | `localhost:7233` |
| `PROVIDER_DELAY` (mock provider) | `500ms`. Raise it to have time to kill the worker mid-charge |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` |
| `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG` | Sample 100%. Use `parentbased_traceidratio` / `0.01` for long load tests |
| `PPROF_ADDR` (Event API, Processor) | Unset. Set to `localhost:6060` to serve Go profiles |

</details>

## Project Structure

```
.
├── Makefile                  # make demo, test, k8s-up, and everything else
├── Dockerfile                # One multi-stage build for any service
├── docker-compose.yml        # Backing services, plus the app under a profile
├── proto/fintech/v1/         # Event schema
├── migrations/               # Postgres schema
├── deploy/                   # Kubernetes manifests; OTel, Prometheus, and Grafana config
├── docs/                     # Performance write-up and roadmap
├── loadtest/k6.js            # k6 load test
├── scripts/                  # Smoke test and Kubernetes helpers
└── src/
    ├── cmd/                  # Entry points: api, processor, query, worker, dlq, loadgen
    ├── gen/                  # Generated Protobuf code
    └── pkg/
        ├── processor/        # Partition workers, batching, retries, DLQ, integration tests
        ├── query/            # Read API, SSE stream, NOTIFY listener, demo page
        ├── workflow/         # Settlement workflow, activities, mock provider, outbox relay
        ├── events/           # Validation shared by the API and the Processor
        ├── telemetry/        # OpenTelemetry setup and Kafka trace propagation
        └── config/           # Environment-based configuration
```

## Not Production-Ready

This is a portfolio project, and these were left out on purpose:

- **Authentication:** anyone who can reach the APIs can read any account or submit transactions.
- **Atomic transfers:** `transfer_in` and `transfer_out` are independent events, so one side can fail alone. A workflow like settlement is the natural fix.
- **Provider reconciliation:** a real provider's timeout is ambiguous because the charge may have gone through, so it would need checking before a refund.
- **Durable infrastructure:** one Kafka broker with replication factor 1, one Postgres, and a Temporal dev server that keeps its state in memory. Real credentials would also come from a secret manager, not committed config.
- **Data retention:** `processed_events` grows without limit.
- **Cloud deployment and operations:** Terraform, autoscaling on consumer lag, and alerting.

## More

- [docs/performance.md](docs/performance.md): how throughput went up about 10×, how the bottleneck was found, and what still limits it
- [docs/ROADMAP.md](docs/ROADMAP.md): the phase-by-phase build plan
