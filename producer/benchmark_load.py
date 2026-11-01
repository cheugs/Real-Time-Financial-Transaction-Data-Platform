"""
Benchmark: send N clean events to Kafka as fast as possible, then measure how
long the pipeline takes to store all of them in Postgres.

Run it through scripts/run_benchmark.py (which also records CPU/memory), or:
    docker compose run --rm producer python benchmark_load.py --events 100000
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone

import psycopg2

from generator import make_event
from producer import TOPIC, make_producer


def pg():
    """Open a connection to Postgres."""
    return psycopg2.connect(host=os.getenv("PG_HOST", "localhost"), dbname=os.getenv("PG_DB"),
                            user=os.getenv("PG_USER"), password=os.getenv("PG_PASSWORD"))


def query(sql, params=None):
    """Run one query and return its first row."""
    conn = pg()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    finally:
        conn.close()


def main():
    """Send N events, wait until all are stored, then measure and save the results."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument("--timeout", type=int, default=3600, help="max seconds to wait for Spark")
    args = parser.parse_args()
    n = args.events
    tag = f"BMK{int(time.time())}"
    started_at = datetime.now(timezone.utc)

    # ---- 1. produce
    print(f"[{tag}] sending {n:,} events to Kafka ...", flush=True)
    producer = make_producer()
    t0 = time.time()
    for i in range(n):
        # one account per event -> the benchmark doesn't trigger the velocity rule
        e = make_event(account_id=f"{tag}-{i}")
        while True:
            try:
                producer.produce(TOPIC, key=e["account_id"].encode(), value=json.dumps(e).encode())
                break
            except BufferError:          # local queue full -> let it drain
                producer.poll(0.1)
        if i % 10_000 == 0:
            producer.poll(0)
    producer.flush()
    produce_s = time.time() - t0
    print(f"[{tag}] produced in {produce_s:.1f}s ({n / produce_s:,.0f} events/s)", flush=True)

    # ---- 2. wait until Spark has stored them all
    # (the normal producer is stopped during a benchmark, so every new row is ours)
    deadline = time.time() + args.timeout
    last = -1
    while time.time() < deadline:
        done = query("select coalesce(sum(rows_inserted), 0) from batch_log "
                     "where query_name = 'raw_sink' and logged_at >= %s", (started_at,))[0]
        if done != last:
            print(f"[{tag}] stored so far: {done:,} / {n:,}", flush=True)
            last = done
        if done >= n:
            break
        time.sleep(5)

    # ---- 3. measure
    count, distinct, first_kafka, last_processed, p50, p95, p_max = query("""
        select count(*), count(distinct event_id), min(kafka_time), max(processed_at),
               percentile_cont(0.5)  within group (order by extract(epoch from processed_at - kafka_time)),
               percentile_cont(0.95) within group (order by extract(epoch from processed_at - kafka_time)),
               max(extract(epoch from processed_at - kafka_time))
        from raw_transactions where account_id like %s
    """, (tag + "-%",))
    e2e_s = (last_processed - first_kafka).total_seconds() if count else None
    avg_eps, max_eps, max_lag, batches = query("""
        select avg(processed_rows_per_second), max(processed_rows_per_second), max(kafka_lag), count(*)
        from stream_progress
        where query_name = 'raw_sink' and num_input_rows > 0 and batch_timestamp >= %s
    """, (started_at,))

    result = {
        "tag": tag,
        "date": started_at.strftime("%Y-%m-%d %H:%M UTC"),
        "events_sent": n,
        "events_stored": count,
        "distinct_event_ids": distinct,
        "lost": n - count,
        "produce_seconds": round(produce_s, 1),
        "produce_events_per_s": round(n / produce_s),
        "end_to_end_seconds": round(e2e_s, 1) if e2e_s else None,
        "end_to_end_events_per_s": round(count / e2e_s) if e2e_s else None,
        "spark_avg_events_per_s": round(avg_eps or 0),
        "spark_max_events_per_s": round(max_eps or 0),
        "spark_batches": batches,
        "max_kafka_lag": max_lag,
        "latency_p50_s": round(p50, 2) if p50 is not None else None,
        "latency_p95_s": round(p95, 2) if p95 is not None else None,
        "latency_max_s": round(float(p_max), 2) if p_max is not None else None,
    }
    print(json.dumps(result, indent=2), flush=True)
    os.makedirs("/results", exist_ok=True)
    with open(f"/results/benchmark_{n}.json", "w") as f:
        json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()
