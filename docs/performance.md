# Performance

How the pipeline went from saturating at about 1,630 events/s to about 15,000–24,000 events/s, what limits it now, and which target it still misses.

**Test environment:** everything on one laptop (10 cores, 8 GB for Docker, on AC power unless noted): one Processor, one Kafka broker, one Postgres, a 6-partition topic, and 1,000 accounts. The load generator shares the machine. Treat these numbers as comparisons between versions of this system, not as capacity figures for real hardware.

End-to-end latency runs from the event's `created_at` to its balance update being committed. Percentiles are estimated from histogram buckets, so they are approximate.

## Summary

| Metric | Target | Before Phase 5 | Now | Status |
| --- | --- | --- | --- | --- |
| Sustained throughput | 5,000 events/s | ~1,630 events/s | 5,000 events/s held for 10 minutes | ✅ Met |
| No lost events | none | none | 3,292,347 accepted, 3,292,347 applied | ✅ Met |
| Saturation point | identify | ~1,630 events/s | ~15,000–24,000 events/s | ✅ Identified |
| End-to-end p50 | < 20 ms | saturated at 5,000/s | 8.5 ms sustained straight to Kafka; 40 ms with k6 and all services on the laptop | ✅ Met straight to Kafka |
| End-to-end p99 | < 50 ms | saturated at 5,000/s | 406 ms sustained straight to Kafka; 3.4 s with k6 and all services on the laptop | ❌ Missed |
| Event API p99 | < 30 ms | | above 30 ms under k6 at 5,000/s (p95 45 ms) | ❌ Missed |

## What changed, one step at a time

Each step was measured on its own. All runs below go straight to Kafka at the stated rate for 60 s.

| Step | Rate | p50 | p95 | p99 | Peak throughput |
| --- | --- | --- | --- | --- | --- |
| Baseline: one transaction per event | 2,000/s | 14.1 s | 28.4 s | 29.7 s | 1,627/s (saturated) |
| Control: new code, batch size capped at 1 | 2,000/s | 11.9 s | 28.2 s | 29.6 s | 1,667/s (saturated) |
| **Batched writes** | 2,000/s | 9.9 ms | 19.3 ms | 33.9 ms | keeps up |
| Batched writes | 5,000/s | 15.1 ms | 47.6 ms | 190.5 ms | keeps up |
| **+ Postgres tuning** (3 runs) | 5,000/s | ~15 ms | 33–48 ms | 63–145 ms | keeps up |
| **+ `linger.ms` 5 → 1** (2 runs) | 5,000/s | 8.3–9.0 ms | 41–58 ms | 87–141 ms | keeps up |

The control run matters most: it runs the new code with batching effectively turned off and reproduces the old limit, so batching accounts for the throughput gain.

## How the bottleneck was found

**The assumption to avoid:** that a slow pipeline needs more CPU. At 1,500 events/s the Processor used about 77% of one core out of 10, and half of that was the Go scheduler idling. Postgres was at about 92% of one core.

**What showed the cause:** sampling what Postgres connections were waiting on.

| Backend state | Samples | Meaning |
| --- | --- | --- |
| `idle in transaction` / `ClientRead` | 101 | Waiting for the next statement: **round trips per event** |
| `Lock: object` | 45 | Waiting for the global `NOTIFY` lock at commit |
| `IO: WalSync` | 36 | **One fsync per commit** |

Each worker spent about 3.7 ms per event on BEGIN, four statements, and COMMIT. Six partitions at that speed gives the ceiling we measured.

### Fix 1: batched writes

A worker now applies everything queued for its partition in one transaction, with a fixed number of round trips and one commit per batch. Events are still decided in order against an in-memory balance, so each one sees the effect of those before it.

There's no timer: a batch is whatever is already waiting. A lone event is processed immediately, so light load keeps low latency, and batches grow only under pressure. The average batch was about 8 events at 5,000/s and about 450 near saturation.

Batching had to keep the correctness guarantees from Phase 3, which shaped three details:

- **Dead letters are published before the commit.** A crash then redelivers the batch and decides it identically. Publishing afterwards could re-decide a rejected event against a balance that already includes later events.
- **Account rows are locked before checking for duplicates**, so work committed by another processor during a rebalance is visible to the check.
- **One bad event can't fail its batch.** If the database refuses a batch because of an event's contents, the events are retried one at a time and only the bad one goes to the DLQ.

### Fix 2: Postgres checkpoints

With default settings, each 60 s run contained 2 checkpoints and p99 was 340–910 ms. Raising `shared_buffers` to 1 GB and `max_wal_size` to 4 GB took checkpoints during those runs to 0 and p99 to 63–145 ms. `synchronous_commit` stayed on, so durability didn't change.

### Fix 3: producer `linger.ms`

Splitting latency by stage showed Kafka taking over half the median, and 5 ms of that was a setting chosen earlier. Going to 1 ms halved the Kafka stage (8 → 3.8 ms) and took the median from about 16 ms to about 9 ms. Runs alternated between the two settings so that any drift would show up.

## What the sustained runs showed

The short runs above look better than the system really is, because most of them contained no checkpoint. A 10-minute hold of the full pipeline (k6 → Event API → Kafka → Processor → Postgres → `NOTIFY` → Query API, with a stream open) gave:

| Measure | Result |
| --- | --- |
| Accepted / applied | 3,292,347 / 3,292,347, with 0 failed and 0 dead-lettered |
| Rate during the hold | ~5,000 events/s (k6 dropped 7,624 of its own iterations, 0.2%) |
| End-to-end p50 / p95 / p99 | 40 ms / 899 ms / 3.4 s |
| Latency to push p50 / p95 / p99 | 36 ms / 377 ms / 1.5 s |
| Stage p50 / p99 | kafka 8 / 304 ms, queue 9 ms / 1.8 s, apply 18 / 927 ms |
| Event API, measured by k6 | p50 6.4 ms, p95 44.6 ms, max 418 ms |

p99 got worse as the run went on: about 0.5 s around minute 4, 1.8 s around minute 9, and a peak of 4.8 s at minute 11, while throughput held steady. Postgres logs show WAL-triggered checkpoints running for most of the hold, because 4 GB of WAL lasted only about 7 minutes at this write rate.

To separate the system from its test setup, the same rate and duration were repeated straight to Kafka, without k6, the Event API, or the Query API on the machine:

| Measure | Full pipeline with k6 | Straight to Kafka |
| --- | --- | --- |
| End-to-end p50 | 40 ms | **8.5 ms** |
| End-to-end p95 | 899 ms | 146 ms |
| End-to-end p99 | 3.4 s | **406 ms** |
| Kafka stage p99 | 304 ms | 18 ms |
| Queue stage p99 | 1.8 s | 187 ms |
| Apply stage p99 | 927 ms | 173 ms |
| p99 over time | climbs to 4.8 s | steady, 190–700 ms |

Two conclusions:

1. **Most of the seconds-long tail came from sharing the laptop.** Removing k6, the Event API, and the Query API took p99 from 3.4 s to 406 ms and removed the upward drift. This comparison can't say how much each of the three contributed, and part of the difference could be a real cost of the HTTP path.
2. **The remaining tail is in the database stages.** Kafka's p99 is only 18 ms here, while `queue` and `apply` sit near 180 ms. `queue` is mostly events waiting for the previous batch to finish, so both point at slow commits. Checkpoints ran for most of this run too, which fits, though the two haven't been isolated.

## Other things tested

**`NOTIFY` does have a cost.** An earlier measurement (Phase 4) found none, but per-event round trips were capping throughput at the time and hid it. Postgres takes a global lock while committing a transaction that sends a `NOTIFY`, which serializes those commits. With batching in place and runs alternating on and off at 5,000/s:

| `NOTIFY` | p50 | p99 (two runs) |
| --- | --- | --- |
| On | 16–20 ms | 512, 910 ms |
| Off | 16–17 ms | 338, 477 ms |

That's no change at the median and roughly 1.5–2× on p99, from only two pairs, so treat it as directional. It's still worth it here because it keeps live updates transactional. An outbox feeding a Kafka topic is the next step if it ever becomes the limit.

**Trace sampling made no clear difference.** At 5,000/s, sampling 1% gave p99 of 82–195 ms against 63–155 ms at 100%, which is within run-to-run noise. Tracing is not a bottleneck at this rate. The long runs used 1% anyway, because local Jaeger can't hold millions of traces.

**The load generator was wrong at first.** With one ticker per request, Go dropped ticks at high rates, so "3,000/s" really sent 2,806/s. It now releases requests in 10 ms batches and catches up, delivering 1,996/s for a 2,000/s target.

**A misleading comparison, caught.** One `NOTIFY`-off run looked much worse than `NOTIFY` on (p99 1.6 s). It ran right after a 32,000/s test, while Postgres was still catching up. Later runs wait for autovacuum to finish, and comparisons alternate.

## Settlements and the hot path

Withdrawals settle through Temporal, but the Processor never calls it: it writes a row to an outbox table in the same transaction as the debit, and a separate worker starts the workflows. To test that isolation, runs alternated between deposits only and a mix where 1 in 6 events is a withdrawal, at 5,000/s with the worker running.

| Run | Events | p50 | p99 | Kept up? |
| --- | --- | --- | --- | --- |
| 1 | Deposits only, no backlog yet | 8 ms | 1.2 s | Yes |
| 2 | With withdrawals | 26 ms | 1.3 s | Yes |
| 3 | Deposits only, backlog still being worked through | 19 ms | 0.9 s | Yes |
| 4 | With withdrawals | 20 ms | 0.6 s | Yes |

- **Throughput is isolated.** The pipeline held 5,000/s with lag draining within 2 s, while 81,000 settlements waited in the outbox.
- **Latency isn't, on one laptop.** p50 rose from about 8 ms to about 20 ms. Deposits-only run 3 slowed as much as the withdrawal runs, which suggests Temporal and the worker competing for the machine rather than the outbox write itself. These runs can't fully separate the two, and p99 is too noisy here to compare.
- **Settlement throughput is far lower than event throughput:** about 30 settlements/s on the single-process Temporal dev server. That's expected for that server and for a mock provider with a 500 ms delay, and it wasn't tuned. A sustained 830 withdrawals/s just grows the backlog, which is what the outbox is there to absorb.

## Saturation

| Rate | Result |
| --- | --- |
| 8,000/s | Keeps up (before Postgres tuning): p50 17 ms, p99 490 ms |
| 12,000/s | Keeps up (before Postgres tuning): p50 18 ms, p99 367 ms |
| 16,000/s | Keeps up (before Postgres tuning): p50 39 ms, p99 862 ms |
| 24,000/s | Barely keeps up (final settings): p50 548 ms, p99 4.9 s, drained 2 s after the load ended |
| 32,000/s | Falls behind (final settings): averaged ~17,000/s with a checkpoint in the run, p50 over 58 s |

**What limits it:** near saturation, most of the wait is in Kafka (the `kafka` stage reaches many seconds while `apply` stays under 0.5 s at p99). Batches reach their 500-event cap, so the Processor is taking events as fast as this setup can fetch and commit them. It's a single process on a laptop that it shares with the broker, the database, and the load generator.

## Known limits

- **The p99 target isn't met.** Sustained p99 is 406 ms in the best setup. The evidence points to slow commits while checkpoints run, but that hasn't been isolated, and the growing table isn't ruled out.
- **Results drift as data grows.** `processed_events` passed 18M rows during testing, with a 1.3 GB primary key index on random UUIDs, larger than `shared_buffers`. Random keys touch pages across the whole index, which means more page writes after each checkpoint. Time-ordered IDs (UUIDv7) or partitioning by time are the obvious next steps, and neither has been tested.
- **One machine.** Real hardware with separate brokers and a dedicated database disk would behave differently.

## Reproducing

```bash
make up && make run-processor                         # in one terminal
make loadgen ARGS="-direct -rate 5000 -duration 60s"  # straight to Kafka
make run-api                                          # then, for the full path:
k6 run -e TARGET=5000 -e HOLD=10m loadtest/k6.js
```

Set `PPROF_ADDR=localhost:6060` to profile a service, and `OTEL_TRACES_SAMPLER=parentbased_traceidratio OTEL_TRACES_SAMPLER_ARG=0.01` for long runs. The Grafana dashboard's **Where latency goes** panel shows the per-stage breakdown.
