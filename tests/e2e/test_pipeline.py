"""
End-to-end tests: Kafka -> Spark -> Postgres -> dbt -> dashboard, on the running stack.

    pytest tests/e2e              # normal scenarios (~3-4 min)
    pytest tests/e2e -m slow      # Postgres outage + replay (slower, replay clears tables!)
"""

import os
import platform
import subprocess
import time
import urllib.request
from datetime import timedelta

import psycopg2.extensions
import pytest

import generator
import queries
from e2e_helpers import ROOT, compose, pg, sql, wait_for


def events_for(tag, n, **kwargs):
    """n valid events whose account_id starts with the test's tag."""
    return [generator.make_event(account_id=f"{tag}-{i}", **kwargs) for i in range(n)]


def stored(tag):
    """(rows, distinct ids) stored for this tag in raw_transactions."""
    return sql("select count(*), count(distinct event_id) from raw_transactions where account_id like %s",
               (tag + "%",))[0]


def wait_stored(tag, n, timeout=180):
    """Wait until at least n events of this tag are stored."""
    return wait_for(lambda: stored(tag)[0] >= n and stored(tag), timeout=timeout,
                    message=f"{n} events of {tag} in raw_transactions")


# ------------------------------------------------------------------ ingestion
def test_valid_events_flow_from_kafka_to_postgres(kafka, tag):
    sent = events_for(tag, 50)
    kafka.send(*sent)
    total, distinct = wait_stored(tag, 50)
    assert (total, distinct) == (50, 50)

    # values are stored unchanged, with an EUR amount
    e = sent[0]
    row = sql("select amount, currency, status, amount_eur, schema_version from raw_transactions "
              "where event_id = %s", (e["event_id"],))[0]
    assert float(row[0]) == pytest.approx(e["amount"])
    assert row[1:3] == (e["currency"], e["status"])
    assert row[3] is not None and row[4] == e["schema_version"]


def test_duplicate_events_are_stored_once(kafka, tag):
    sent = events_for(tag, 20)
    kafka.send(*sent)
    kafka.send(*sent)                       # every event sent a second time
    wait_stored(tag, 20)
    # give Spark time to process the second copies too
    wait_for(lambda: sql("select coalesce(sum(duplicates_skipped),0) from batch_log "
                         "where logged_at > now() - interval '3 minutes'")[0][0] >= 20,
             timeout=120, message="duplicates to be counted in batch_log")
    assert stored(tag) == (20, 20)


def test_invalid_events_are_isolated_with_a_reason(kafka, tag):
    good = events_for(tag, 3)
    bad = []
    for i, (field, value, reason) in enumerate([
        ("amount", -5, "non_positive_amount"),
        ("currency", "XXX", "unknown_currency"),
        ("event_time", "not-a-date", "invalid_event_time"),
        ("status", "MAYBE", "invalid_status"),
    ]):
        e = generator.make_event(account_id=f"{tag}-bad{i}")
        e[field] = value
        bad.append((e, reason))
    malformed = '{"event_id": "%s-malformed", "amount": ' % tag

    kafka.send(*good, *[e for e, _ in bad], malformed)
    wait_stored(tag, 3)
    wait_for(lambda: sql("select count(*) from bad_events where raw_value like %s",
                         (f"%{tag}%",))[0][0] >= 5, timeout=120, message="invalid events in bad_events")

    reasons = dict(sql("select event_id, error_reason from bad_events where raw_value like %s",
                       (f"%{tag}%",)))
    for e, reason in bad:
        assert reasons[e["event_id"]] == reason
    assert "malformed_json" in reasons.values()
    assert stored(tag)[0] == 3              # only the good ones reached raw_transactions


def test_schema_evolution_v1_v2_and_unknown_v3(kafka, tag):
    v1 = generator.make_event(account_id=f"{tag}-1", schema_version=1)
    v2 = generator.make_event(account_id=f"{tag}-2", schema_version=2)
    v3 = generator.make_event(account_id=f"{tag}-3", schema_version=2)
    v3.update(schema_version=3, loyalty_points=42)          # a field Spark doesn't know yet
    kafka.send(v1, v2, v3)
    wait_stored(tag, 3)
    rows = dict(sql("select schema_version, payment_method from raw_transactions where account_id like %s",
                    (tag + "%",)))
    assert rows[1] is None and rows[2] is not None and rows[3] is not None


def test_late_event_is_kept_and_counted_in_its_window(kafka, tag):
    late_time = generator.now_utc() - timedelta(minutes=3)
    e = generator.make_event(account_id=f"{tag}-late", event_time=late_time)
    kafka.send(e)
    wait_stored(tag, 1)
    delay = sql("select extract(epoch from kafka_time - event_time) from raw_transactions "
                "where event_id = %s", (e["event_id"],))[0][0]
    assert float(delay) > 170
    window = late_time.replace(second=0, microsecond=0)
    wait_for(lambda: sql("select txn_count from txn_volume_per_minute where window_start = %s",
                         (window,)), timeout=60, message="the late event's window")


# ------------------------------------------------------------------ monitoring + dashboard
def test_monitoring_tables_are_filled():
    lag, eps = sql("select kafka_lag, processed_rows_per_second from stream_progress "
                   "where query_name = 'raw_sink' order by batch_timestamp desc limit 1")[0]
    assert lag is not None and lag >= 0
    assert eps is not None
    starts = sql("select count(*) from pipeline_events "
                 "where event_type in ('fresh_start', 'restart_from_checkpoint')")[0][0]
    assert starts >= 1


def test_dashboard_figures_match_the_database():
    conn = pg()
    # one consistent snapshot, so the stream writing in the background can't change numbers between queries
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ)
    try:
        k = queries.kpis(conn)
        vol = queries.volume_per_minute(conn, minutes=60)
        with conn.cursor() as cur:
            cur.execute("select count(distinct event_id), coalesce(sum(amount_eur), 0) from raw_transactions")
            n, total = cur.fetchone()
            cur.execute("select count(*) from bad_events")
            invalid = cur.fetchone()[0]
            cur.execute("select coalesce(sum(rows_received - rows_inserted), 0) from batch_log "
                        "where query_name = 'raw_sink'")
            dups = cur.fetchone()[0]
            cur.execute("select coalesce(sum(txn_count), 0) from txn_volume_per_minute "
                        "where window_start > now() - interval '60 minutes'")
            window_total = cur.fetchone()[0]
    finally:
        conn.close()

    assert k["total_events"] == n
    assert float(k["total_amount_eur"]) == pytest.approx(float(total))
    assert k["invalid_events"] == invalid
    assert k["duplicates_skipped"] == dups
    assert int(vol["txn_count"].sum()) == window_total

    # and the dashboard web app itself is up
    with urllib.request.urlopen("http://localhost:8501/_stcore/health", timeout=10) as r:
        assert r.status == 200


def test_dbt_models_build_and_data_tests_pass():
    result = compose("exec", "-T", "dbt", "sh", "-c", "dbt run --quiet && dbt test", timeout=300)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-2000:]
    assert "ERROR=0" in result.stdout


# ------------------------------------------------------------------ failure scenarios
def test_spark_restart_does_not_lose_or_duplicate_events(kafka, tag):
    before = sql("select coalesce(max(id), 0) from pipeline_events")[0][0]
    kafka.send(*events_for(tag, 100))
    assert compose("restart", "spark").returncode == 0        # crash/restart in the middle
    kafka.send(*[generator.make_event(account_id=f"{tag}-b{i}") for i in range(100)])

    assert wait_stored(tag, 200, timeout=300) == (200, 200)    # nothing lost, nothing doubled
    events = [r[0] for r in sql("select event_type from pipeline_events where id > %s", (before,))]
    assert "restart_from_checkpoint" in events                 # the recovery is traceable


@pytest.mark.slow
def test_postgres_outage_is_retried_without_losing_events(kafka, tag):
    assert compose("stop", "postgres").returncode == 0
    try:
        kafka.send(*events_for(tag, 50))     # Kafka keeps them while the database is down
        time.sleep(20)
    finally:
        assert compose("start", "postgres").returncode == 0
    assert wait_stored(tag, 50, timeout=420) == (50, 50)


@pytest.mark.slow
def test_replay_rebuilds_the_same_events(kafka, tag):
    kafka.send(*events_for(tag, 30))
    wait_stored(tag, 30)
    ids_before = {r[0] for r in sql("select event_id from raw_transactions where account_id like %s",
                                    (tag + "%",))}

    if platform.system() == "Windows":
        cmd = ["powershell", "-ExecutionPolicy", "Bypass", "-File", os.path.join("scripts", "replay.ps1")]
    else:
        cmd = ["sh", os.path.join("scripts", "replay.sh")]
    assert subprocess.run(cmd, cwd=ROOT, timeout=300).returncode == 0
    assert stored(tag)[0] == 0                                   # tables were really cleared

    wait_stored(tag, 30, timeout=600)                            # ... and rebuilt from Kafka
    ids_after = {r[0] for r in sql("select event_id from raw_transactions where account_id like %s",
                                   (tag + "%",))}
    assert ids_after == ids_before
