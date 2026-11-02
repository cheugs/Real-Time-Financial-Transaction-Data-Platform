"""
All SQL used by the dashboard, in one place.
Kept separate from app.py so the end-to-end tests can check that the
numbers on the dashboard match the database.

Demo mode (DEMO_MODE=1, used for the online version): the database holds a
snapshot of a real pipeline run, so "the last hour" is counted back from the
moment the snapshot was taken (table demo_meta) instead of from now().
The times shown on the charts are then moved forward by (now - snapshot time),
so the newest data appears as "now". The dashboard says this in a banner.
"""

import os

import pandas as pd
import psycopg2

DEMO_MODE = os.getenv("DEMO_MODE", "0") == "1"
# the "current time" used in every time filter
NOW = "(select snapshot_at from demo_meta)" if DEMO_MODE else "now()"
# added to the time columns we display (demo mode only)
SHIFT = " + (now() - (select snapshot_at from demo_meta))" if DEMO_MODE else ""


def connect():
    """Open a new connection to Postgres (settings come from environment variables)."""
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        dbname=os.getenv("PG_DB", "fintech"),
        user=os.getenv("PG_USER", "fintech"),
        password=os.getenv("PG_PASSWORD", ""),
        port=int(os.getenv("PG_PORT", "5432")),
        sslmode=os.getenv("PG_SSLMODE", "prefer"),   # cloud databases need "require"
    )


def read(conn, sql):
    """Run a SQL query and return the result as a pandas DataFrame."""
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


KPI_SQL = f"""
select
    (select count(*) from raw_transactions)                                   as total_events,
    (select coalesce(sum(amount_eur), 0) from raw_transactions)                as total_amount_eur,
    (select coalesce(sum(duplicates_skipped), 0) from batch_log
      where query_name = 'raw_sink')                                          as duplicates_skipped,
    (select count(*) from bad_events)                                         as invalid_events,
    (select count(*) from raw_transactions
      where kafka_time - event_time > interval '60 seconds')                  as late_events,
    (select count(*) from raw_transactions
      where kafka_time - event_time > interval '10 minutes')                  as beyond_watermark,
    (select round(avg(extract(epoch from processed_at - kafka_time))::numeric, 2)
       from raw_transactions
      where processed_at > {NOW} - interval '5 minutes')                      as latency_5min_s,
    (select kafka_lag from stream_progress where query_name = 'raw_sink'
      order by batch_timestamp desc limit 1)                                  as kafka_lag,
    (select round(avg(processed_rows_per_second)::numeric, 1) from stream_progress
      where query_name = 'raw_sink' and num_input_rows > 0
        and batch_timestamp > {NOW} - interval '5 minutes')                   as throughput_eps,
    (select greatest(count(distinct run_id) - 1, 0) from stream_progress
      where query_name = 'raw_sink')                                          as restarts,
    (select extract(epoch from {NOW} - max(batch_timestamp)) from stream_progress
      where query_name = 'raw_sink')                                          as last_batch_age_s
"""


def kpis(conn):
    """All-time numbers for the KPI blocks (one row as a dict)."""
    return read(conn, KPI_SQL).iloc[0].to_dict()


def kpis_in_range(conn, minutes=60):
    """Transactions in the selected time range (by event time)."""
    return read(conn, f"""
        select count(*)                                           as txn_count,
               coalesce(sum(amount_eur), 0)                       as amount_eur,
               coalesce(avg(amount_eur), 0)                       as avg_amount_eur,
               count(*) filter (where status = 'APPROVED')        as approved,
               count(*) filter (where status = 'DECLINED')        as declined,
               count(distinct account_id)                         as accounts
        from raw_transactions
        where event_time > {NOW} - interval '{int(minutes)} minutes' and event_time <= {NOW}
    """).iloc[0].to_dict()


def volume_per_minute(conn, minutes=60):
    """Spark's per-minute aggregates for the last `minutes` minutes."""
    return read(conn, f"""
        select window_start{SHIFT} as window_start, txn_count, total_amount_eur, approved_count, declined_count,
               avg_arrival_delay_s, late_event_count
        from txn_volume_per_minute
        where window_start > {NOW} - interval '{int(minutes)} minutes' and window_start <= {NOW}
        order by window_start
    """)


def stream_progress(conn, minutes=60):
    """Spark progress reports: throughput and Kafka lag per micro-batch."""
    return read(conn, f"""
        select batch_timestamp{SHIFT} as batch_timestamp, processed_rows_per_second, input_rows_per_second,
               kafka_lag, batch_duration_ms
        from stream_progress
        where query_name = 'raw_sink' and batch_timestamp > {NOW} - interval '{int(minutes)} minutes' and batch_timestamp <= {NOW}
        order by batch_timestamp
    """)


def pipeline_health(conn, minutes=60):
    """Latency, late events and schema versions per minute (dbt model)."""
    return read(conn, f"""
        select minute{SHIFT} as minute, avg_processing_latency_s, max_processing_latency_s,
               avg_arrival_delay_s, late_events, schema_v1_events, schema_v2_events
        from analytics.pipeline_health_per_minute
        where minute > {NOW} - interval '{int(minutes)} minutes' and minute <= {NOW}
        order by minute
    """)


def status_distribution(conn, minutes=60):
    """Number and EUR amount of transactions per status (dbt model)."""
    return read(conn, f"""
        select status, sum(txn_count) as txn_count, round(sum(amount_eur), 2) as amount_eur
        from analytics.status_per_minute
        where minute > {NOW} - interval '{int(minutes)} minutes' and minute <= {NOW}
        group by status order by status
    """)


def volume_by_currency(conn):
    """Volume per currency, in local money and in EUR (dbt model)."""
    return read(conn, """
        select currency, rate_to_eur, txn_count, total_amount_local, total_amount_eur
        from analytics.volume_by_currency order by total_amount_eur desc
    """)


def invalid_by_reason(conn):
    """Number of rejected records per error reason."""
    return read(conn, "select error_reason, count(*) as events from bad_events group by 1 order by 2 desc")


def suspicious(conn, minutes=60):
    """Simulated suspicious-activity indicators (dbt model)."""
    return read(conn, f"""
        select rule, account_id, window_start{SHIFT} as window_start, txn_count, total_amount_eur, details
        from analytics.suspicious_activity
        where window_start > {NOW} - interval '{int(minutes)} minutes' and window_start <= {NOW}
        order by window_start desc
    """)


def pipeline_events(conn, limit=10):
    """Latest starts, restarts, failures and replays of the pipeline."""
    return read(conn, f"""
        select logged_at{SHIFT} as logged_at, event_type, query_name, left(details, 200) as details
        from pipeline_events order by logged_at desc limit {int(limit)}
    """)


def snapshot_info(conn):
    """When the demo snapshot was taken (demo mode only), as a dict."""
    return read(conn, "select snapshot_at, source from demo_meta").iloc[0].to_dict()
