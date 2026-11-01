"""
PySpark Structured Streaming job: Kafka -> Postgres.

Runs two streaming queries at the same time:

  1. raw_sink        - stores every valid event in raw_transactions
                       (duplicates are ignored using the event_id primary key)
                       and puts invalid / malformed messages in bad_events.

  2. volume_per_min  - counts transactions per 1-minute EVENT-TIME window,
                       with a 10-minute watermark for late events,
                       and upserts the result into txn_volume_per_minute.

Both queries use checkpoints, so if Spark restarts it carries on from the
last Kafka offset it finished. Because both sinks are idempotent
(INSERT ... ON CONFLICT), re-processing a batch does not create duplicates.

While running, the main thread saves each query's progress (throughput,
batch duration, Kafka lag) into stream_progress, and start/failure events
into pipeline_events, so the dashboard can monitor the pipeline.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from transformations import add_validation, parse_events, volume_per_window

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("TOPIC", "payments")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints")
WATERMARK = os.getenv("WATERMARK", "10 minutes")         # how long we wait for late events
WINDOW = os.getenv("WINDOW", "1 minute")                 # size of the aggregation window
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "5 seconds")
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "20000"))

PG = dict(
    host=os.getenv("PG_HOST", "localhost"),
    dbname=os.getenv("PG_DB", "fintech"),
    user=os.getenv("PG_USER", "fintech"),
    password=os.getenv("PG_PASSWORD", ""),
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("py4j").setLevel(logging.WARNING)   # it prints too many messages at INFO level
log = logging.getLogger("streaming_job")


# ------------------------------------------------------------------ helpers
def with_retry(action, what, attempts=3, wait_s=2):
    """Retry a database action a few times (e.g. Postgres restarting).
    If it still fails, the error goes up to Spark, the query stops and the
    container restarts -> the same batch is processed again from the checkpoint."""
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except psycopg2.OperationalError as e:
            log.warning("%s failed (attempt %d/%d): %s", what, attempt, attempts, e)
            if attempt == attempts:
                raise
            time.sleep(wait_s * attempt)


def pg_connect():
    """Open a new connection to Postgres."""
    return psycopg2.connect(**PG)


def log_pipeline_event(event_type, query_name=None, run_id=None, details=None):
    """Write a row to pipeline_events (start, restart, failure...) and to the log."""
    log.info("pipeline event: %s %s %s", event_type, query_name or "", details or "")

    def insert():
        """The database work, wrapped so with_retry() can run it again."""
        with pg_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pipeline_events (event_type, query_name, run_id, details) VALUES (%s, %s, %s, %s)",
                (event_type, query_name, run_id, details),
            )
    try:
        with_retry(insert, "log pipeline event")
    except Exception as e:  # monitoring must never crash the pipeline
        log.error("could not save pipeline event: %s", e)


def load_fx_rates():
    """Read the exchange rates from Postgres once, at start-up."""
    def read():
        """The database work, wrapped so with_retry() can run it again."""
        with pg_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT currency, rate_to_eur FROM fx_rates")
            return {cur_: float(rate) for cur_, rate in cur.fetchall()}
    rates = with_retry(read, "load fx rates", attempts=10, wait_s=3)
    log.info("loaded %d fx rates: %s", len(rates), rates)
    return rates


def last_committed_batch(query_name):
    """Look in the checkpoint folder to see if we are resuming after a restart."""
    commits = os.path.join(CHECKPOINT_DIR, query_name, "commits")
    if not os.path.isdir(commits):
        return None
    ids = [int(f) for f in os.listdir(commits) if f.isdigit()]
    return max(ids) if ids else None


def log_batch(cur, query_name, batch_id, received, inserted, bad=0):
    """Save how many rows a micro-batch received and inserted."""
    # Same transaction as the data: if the batch is rolled back, so is this row.
    # ON CONFLICT DO NOTHING: if Spark re-runs a batch that was already committed,
    # we keep the original numbers.
    cur.execute(
        """INSERT INTO batch_log (query_name, batch_id, rows_received, rows_inserted,
                                  duplicates_skipped, bad_rows)
           VALUES (%s, %s, %s, %s, %s, %s)
           ON CONFLICT (query_name, batch_id) DO NOTHING""",
        (query_name, batch_id, received, inserted, received - inserted, bad),
    )


# ------------------------------------------------------------------ sink 1
def write_raw(batch_df, batch_id):
    """Called by Spark once per micro-batch for the raw query."""
    processed_at = datetime.now(timezone.utc)
    rows = batch_df.collect()   # OK for a student-sized stream (see docs/PERFORMANCE.md)

    good, bad = [], []
    for r in rows:
        if r.error_reason is None:
            good.append((
                r.event_id, r.schema_version, r.account_id, r.merchant_id, r.amount,
                r.currency, r.amount_eur, r.country, r.status, r.payment_method, r.device_id,
                r.event_time, r.kafka_time, processed_at, r.kafka_partition, r.kafka_offset,
            ))
        else:
            bad.append((r.kafka_partition, r.kafka_offset, r.event_id, r.error_reason,
                        r.raw_value, processed_at))

    def write():
        """The database work, wrapped so with_retry() can run it again."""
        with pg_connect() as conn, conn.cursor() as cur:
            inserted = []
            if good:
                # ON CONFLICT DO NOTHING = duplicate events are silently skipped.
                # RETURNING tells us how many rows were really new.
                inserted = execute_values(
                    cur,
                    """INSERT INTO raw_transactions (event_id, schema_version, account_id, merchant_id,
                           amount, currency, amount_eur, country, status, payment_method, device_id,
                           event_time, kafka_time, processed_at, kafka_partition, kafka_offset)
                       VALUES %s
                       ON CONFLICT (event_id) DO NOTHING
                       RETURNING event_id""",
                    good, fetch=True, page_size=5000,
                )
            if bad:
                execute_values(
                    cur,
                    """INSERT INTO bad_events (kafka_partition, kafka_offset, event_id, error_reason,
                           raw_value, processed_at)
                       VALUES %s
                       ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING""",
                    bad,
                )
            log_batch(cur, "raw_sink", batch_id, len(good), len(inserted), len(bad))
            return len(inserted)

    new = with_retry(write, f"raw_sink batch {batch_id}")
    log.info("[raw_sink] batch %d: %d valid, %d new, %d duplicates, %d invalid",
             batch_id, len(good), new, len(good) - new, len(bad))


# ------------------------------------------------------------------ sink 2
def write_volume(batch_df, batch_id):
    """Upsert the per-minute aggregates (update mode sends changed windows only)."""
    rows = batch_df.collect()
    data = [
        (r.window_start, r.window_end, r.txn_count, r.total_amount_eur, r.approved_count,
         r.declined_count, r.avg_arrival_delay_s, r.max_arrival_delay_s, r.late_event_count)
        for r in rows
    ]

    def write():
        """The database work, wrapped so with_retry() can run it again."""
        with pg_connect() as conn, conn.cursor() as cur:
            if data:
                execute_values(
                    cur,
                    """INSERT INTO txn_volume_per_minute (window_start, window_end, txn_count,
                           total_amount_eur, approved_count, declined_count,
                           avg_arrival_delay_s, max_arrival_delay_s, late_event_count)
                       VALUES %s
                       ON CONFLICT (window_start) DO UPDATE SET
                           txn_count = EXCLUDED.txn_count,
                           total_amount_eur = EXCLUDED.total_amount_eur,
                           approved_count = EXCLUDED.approved_count,
                           declined_count = EXCLUDED.declined_count,
                           avg_arrival_delay_s = EXCLUDED.avg_arrival_delay_s,
                           max_arrival_delay_s = EXCLUDED.max_arrival_delay_s,
                           late_event_count = EXCLUDED.late_event_count,
                           updated_at = now()""",
                    data,
                )
            log_batch(cur, "volume_per_min", batch_id, len(data), len(data))

    with_retry(write, f"volume_per_min batch {batch_id}")
    log.info("[volume_per_min] batch %d: %d windows updated", batch_id, len(data))


# ------------------------------------------------------------------ monitoring
def _offsets(value):
    """Kafka offsets in a progress report: {"payments": {"0": 123, "1": 456}}"""
    if isinstance(value, str):
        value = json.loads(value)
    return value or {}


def kafka_lag(progress):
    """Messages in Kafka that this query has not read yet = latest offset - read offset."""
    try:
        src = progress["sources"][0]
        latest, end = _offsets(src.get("latestOffset")), _offsets(src.get("endOffset"))
        return sum(
            int(latest[topic][p]) - int(end.get(topic, {}).get(p, 0))
            for topic in latest for p in latest[topic]
        )
    except Exception:
        return None


def save_progress(queries, seen):
    """Save new progress reports of each query to stream_progress."""
    rows = {}
    for q in queries:
        for p in q.recentProgress:
            if (p["name"], p["runId"], p["timestamp"]) in seen:
                continue
            seen.add((p["name"], p["runId"], p["timestamp"]))
            # When idle, Spark also sends reports for the next batch id, so the
            # same key can appear twice: keep the latest report.
            rows[(p["name"], p["runId"], p["batchId"])] = (
                p["name"], p["runId"], p["batchId"], p["timestamp"], p["numInputRows"],
                p.get("inputRowsPerSecond"), p.get("processedRowsPerSecond"),
                p.get("durationMs", {}).get("triggerExecution"), kafka_lag(p),
            )
    if not rows:
        return
    rows = list(rows.values())

    def write():
        """The database work, wrapped so with_retry() can run it again."""
        with pg_connect() as conn, conn.cursor() as cur:
            execute_values(
                cur,
                """INSERT INTO stream_progress (query_name, run_id, batch_id, batch_timestamp,
                       num_input_rows, input_rows_per_second, processed_rows_per_second,
                       batch_duration_ms, kafka_lag)
                   VALUES %s
                   ON CONFLICT (query_name, run_id, batch_id) DO UPDATE SET
                       batch_timestamp = EXCLUDED.batch_timestamp,
                       num_input_rows = GREATEST(stream_progress.num_input_rows, EXCLUDED.num_input_rows),
                       input_rows_per_second = EXCLUDED.input_rows_per_second,
                       processed_rows_per_second = EXCLUDED.processed_rows_per_second,
                       batch_duration_ms = EXCLUDED.batch_duration_ms,
                       kafka_lag = EXCLUDED.kafka_lag""",
                rows,
            )
    try:
        with_retry(write, "save progress")
    except Exception as e:
        log.error("could not save progress: %s", e)


# ------------------------------------------------------------------ main
def main():
    """Start Spark, start both streaming queries and monitor them."""
    spark = (
        SparkSession.builder.appName("financial-transactions-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")   # small data -> few partitions
        # RocksDB keeps the dedup/window state on disk instead of in memory
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    for name in ("raw_sink", "volume_per_min"):
        last = last_committed_batch(name)
        if last is None:
            log_pipeline_event("fresh_start", name, details="no checkpoint found, reading Kafka from earliest offset")
        else:
            log_pipeline_event("restart_from_checkpoint", name, details=f"resuming after committed batch {last}")

    fx_rates = load_fx_rates()

    # 1. Read from Kafka. startingOffsets=earliest is only used the very first
    #    time; after that Spark uses the offsets saved in the checkpoint.
    #    failOnDataLoss=true: if offsets we still needed were deleted from Kafka,
    #    stop with an error instead of silently skipping transactions.
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "true")
        .load()
        .select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("timestamp").alias("kafka_time"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
    )

    # 2. Parse JSON and check the data-quality rules.
    events = add_validation(parse_events(kafka_df), fx_rates)

    # ---- Query 1: raw events (no watermark: we keep EVERY valid event, even very late ones)
    raw_query = (
        events.writeStream.queryName("raw_sink")
        .foreachBatch(write_raw)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw_sink")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    # ---- Query 2: per-minute volumes using EVENT TIME (valid events only)
    volumes = volume_per_window(events.where(F.col("error_reason").isNull()), WATERMARK, WINDOW)
    volume_query = (
        volumes.writeStream.queryName("volume_per_min")
        .outputMode("update")   # only send windows that changed in this batch
        .foreachBatch(write_volume)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/volume_per_min")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    log.info("streaming queries started: %s, %s", raw_query.name, volume_query.name)

    # Every 10 s: save progress. If a query fails, log it and exit with an error
    # so Docker restarts the container (it then resumes from the checkpoint).
    seen = set()
    try:
        while not spark.streams.awaitAnyTermination(10):
            save_progress(spark.streams.active, seen)
    except Exception as e:
        log_pipeline_event("query_failed", details=str(e)[:2000])
        raise


if __name__ == "__main__":
    main()
