"""Unit tests for the event-time window aggregation (Spark, batch mode).
Checks the numbers are mathematically correct and duplicates don't inflate them."""

import pytest
from pyspark.sql import functions as F

from helpers import FX_RATES, event, kafka_df
from transformations import add_validation, parse_events, volume_per_window


def aggregate(spark, events, kafka_time="2026-01-01 10:00:00"):
    """Run validation + window aggregation and return {'HH:MM': row}."""
    df = add_validation(parse_events(kafka_df(spark, events, kafka_time)), FX_RATES)
    result = volume_per_window(df.where(F.col("error_reason").isNull())).collect()
    return {str(r.window_start)[11:16]: r for r in result}   # key = "HH:MM"


def test_counts_and_amounts_per_window(spark):
    w = aggregate(spark, [
        event("a", "2026-01-01T10:00:10Z", amount=10),
        event("b", "2026-01-01T10:00:50Z", amount=20),
        event("c", "2026-01-01T10:01:30Z", amount=100, currency="GBP"),   # 117 EUR
    ])
    assert w["10:00"].txn_count == 2
    assert float(w["10:00"].total_amount_eur) == pytest.approx(30.0)
    assert w["10:01"].txn_count == 1
    assert float(w["10:01"].total_amount_eur) == pytest.approx(117.0)


def test_window_boundaries(spark):
    w = aggregate(spark, [
        event("a", "2026-01-01T10:00:59.999Z"),
        event("b", "2026-01-01T10:01:00.000Z"),   # start is inclusive -> next window
    ])
    assert w["10:00"].txn_count == 1
    assert w["10:01"].txn_count == 1
    assert str(w["10:00"].window_end) == "2026-01-01 10:01:00"


def test_duplicates_do_not_inflate_aggregates(spark):
    e = event("same-id", "2026-01-01T10:00:10Z", amount=500)
    w = aggregate(spark, [e, e, e, event("other", "2026-01-01T10:00:20Z", amount=1)])
    assert w["10:00"].txn_count == 2
    assert float(w["10:00"].total_amount_eur) == pytest.approx(501.0)


def test_status_counts(spark):
    w = aggregate(spark, [
        event("a", "2026-01-01T10:00:10Z", status="APPROVED"),
        event("b", "2026-01-01T10:00:11Z", status="APPROVED"),
        event("c", "2026-01-01T10:00:12Z", status="DECLINED"),
        event("d", "2026-01-01T10:00:13Z", status="PENDING"),
    ])
    assert (w["10:00"].approved_count, w["10:00"].declined_count, w["10:00"].txn_count) == (2, 1, 4)


def test_arrival_delay_and_late_count(spark):
    # Kafka received both at 10:05:00 -> delays of 270 s and 30 s
    w = aggregate(spark, [
        event("late", "2026-01-01T10:00:30Z"),
        event("ontime", "2026-01-01T10:04:30Z"),
    ], kafka_time="2026-01-01 10:05:00")
    assert w["10:00"].late_event_count == 1
    assert w["10:00"].max_arrival_delay_s == pytest.approx(270.0)
    assert w["10:04"].late_event_count == 0
    assert w["10:04"].avg_arrival_delay_s == pytest.approx(30.0)


def test_mixed_currencies_are_summed_in_eur(spark):
    w = aggregate(spark, [
        event("eur", "2026-01-01T10:00:01Z", amount=100, currency="EUR"),
        event("xaf", "2026-01-01T10:00:02Z", amount=655.957, currency="XAF"),   # = 1 EUR
        event("usd", "2026-01-01T10:00:03Z", amount=100, currency="USD"),       # = 92 EUR
    ])
    assert float(w["10:00"].total_amount_eur) == pytest.approx(193.0, abs=0.01)
