# Performance Results

[Français](../PERFORMANCE.md) · **English**

## How it was measured

```bash
python scripts/run_benchmark.py 100000
```

```bash
python scripts/run_benchmark.py 1000000
```

1. The normal producer is stopped, so only benchmark events are measured.
2. `producer/benchmark_load.py` sends N clean events to Kafka as fast as it can. Each event has its own account, so it doesn't trigger the velocity rule.
3. It waits until `batch_log` shows that all N events were inserted, then reads from Postgres and `stream_progress`:
   - stored count, distinct IDs, lost events
   - **end-to-end throughput**: N ÷ (last `processed_at` − first `kafka_time`)
   - **Spark throughput**: `processedRowsPerSecond` reported by Spark
   - **latency**: `processed_at − kafka_time` (p50 / p95 / max)
   - **max Kafka lag**: messages waiting in Kafka
4. `docker stats` is sampled every 5 s for CPU and memory per container.

Raw results: [benchmarks/benchmark_100000.json](../../benchmarks/benchmark_100000.json) and
[benchmarks/benchmark_1000000.json](../../benchmarks/benchmark_1000000.json).

**Setup:** a Windows 11 laptop running Docker Desktop (12 CPUs, 8 GB for Docker). Spark runs in
`local[4]` with a 2 GB driver, `maxOffsetsPerTrigger=20000` and a 5 s trigger. There is one Kafka
broker with 3 partitions.

## Results

| | 100,000 events | 1,000,000 events |
|---|---|---|
| Events stored / lost / duplicated | 100,000 / **0** / **0** | 1,000,000 / **0** / **0** |
| Producer → Kafka | 41,900 events/s (2.4 s) | 43,900 events/s (22.8 s) |
| Spark throughput (avg / max per batch) | 4,445 / 5,839 events/s | 4,366 / 7,782 events/s |
| **End-to-end throughput** | **3,725 events/s** (26.8 s) | **2,986 events/s** (5 min 35 s) |
| Latency Kafka → Postgres p50 / p95 / max | 10.9 / 20.6 / 24.5 s | 159 / 281 / 312 s |
| Max Kafka consumer lag | 60,003 messages | 940,005 messages |
| Spark micro-batches | 4 | 49 |
| Spark CPU avg / max · memory max | 103% / 237% · 1.3 GB | 111% / 751% · 1.8 GB |
| Postgres CPU avg / max · memory max | 13% / 51% · 86 MB | 53% / 204% · 734 MB |
| Kafka CPU avg / max · memory max | 40% / 207% · 432 MB | 24% / 209% · 1.1 GB |

(100% CPU = one core.)

**Normal load** (the 5 events/s producer): latency p50 = **2.5 s**, p95 = **4.8 s**. This is mostly the 5-second trigger interval.

## How to read the latency numbers

During a benchmark all N events are put into Kafka in a few seconds, but Spark reads at most
20,000 per micro-batch. So most events **wait in Kafka**, and the long latency is queueing time
(you can see it in the Kafka lag), not slow processing. With 1M events the last event waited about
5 minutes. The pipeline never fell over: lag went up to 940k and then back down to 0, and every
event was stored exactly once.

## Bottlenecks

1. **The window/dedup query (`volume_per_min`).** It takes about 10 s per 20k-row batch, compared
   with about 6 s for the raw sink. Each batch shuffles the data and updates the dedup state (one
   key per `event_id` for 10 minutes) in RocksDB. With 1M events in a few minutes, that state holds
   about a million keys. Both queries share the same 4 local cores.
2. **`foreachBatch` + `collect()` in the raw sink.** Each batch is brought to the driver, turned
   into Python objects, and inserted with a single psycopg2 connection. This is single-threaded and
   explains why Spark's *average* CPU is only about 1.1 cores, even though it can use 4.
3. **Postgres inserts.** Postgres has to keep 3 indexes up to date, check the constraints and run
   `RETURNING`. It also has to serve the dbt rebuild every 30 s. It reached about 2 cores at peak:
   busy, but not the limit.
4. **Not a bottleneck:** Kafka (single broker, about 44k events/s ingest) and the producer.

## What would make it faster (not done, to keep the project simple)

- Write from the executors in parallel (`foreachPartition`, or Postgres `COPY`) instead of `collect()`.
- Use more Kafka partitions and Spark cores, or a real Spark cluster instead of `local[4]`.
- Use a shorter dedup horizon (`dropDuplicatesWithinWatermark`) to shrink the state.
- Raise `maxOffsetsPerTrigger` to use fewer, bigger batches. This gives higher throughput but higher latency per batch.
- Use incremental dbt models instead of full rebuilds.

## Conclusion

At student scale the pipeline easily handles the normal load (about 5 events/s with roughly 2.5 s
latency). It processed bursts of 100k and 1M events at about 3,000–3,700 events/s end-to-end, with
**no lost and no duplicated transactions**.
