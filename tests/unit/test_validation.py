"""Unit tests for parsing + data-quality rules (Spark, batch mode)."""

import json

import pytest

import generator
from helpers import FX_RATES, event, kafka_df
from transformations import add_validation, parse_events

T = "2026-01-01T10:00:30Z"


def validate(spark, events):
    """Parse and validate events, return the result rows."""
    return add_validation(parse_events(kafka_df(spark, events)), FX_RATES).collect()


def test_valid_event_passes_and_is_converted_to_eur(spark):
    (row,) = validate(spark, [event("e1", T, amount=100.0, currency="GBP")])
    assert row.error_reason is None
    assert row.amount_eur == pytest.approx(117.0)
    assert str(row.event_time) == "2026-01-01 10:00:30"


def without(e, field):
    """Copy of an event without one field."""
    e = dict(e)
    del e[field]
    return e


@pytest.mark.parametrize("raw, reason", [
    ('{"event_id": "broken", "amount": ', "malformed_json"),
    ("this is not json", "malformed_json"),
    (without(event("e", T), "event_id"), "missing_event_id"),
    (without(event("e", T), "account_id"), "missing_account_id"),
    (without(event("e", T), "amount"), "missing_amount"),
    (without(event("e", T), "event_time"), "missing_event_time"),
    (event("e", "not-a-date"), "invalid_event_time"),
    (event("e", "2999-01-01T00:00:00Z"), "event_time_in_future"),
    (event("e", T, amount=-5), "non_positive_amount"),
    (event("e", T, amount=0), "non_positive_amount"),
    (event("e", T, currency="XXX"), "unknown_currency"),
    (without(event("e", T), "currency"), "unknown_currency"),
    (event("e", T, amount=5_000_000), "amount_too_large"),
    (event("e", T, status="MAYBE"), "invalid_status"),
    (without(event("e", T), "status"), "invalid_status"),
])
def test_invalid_records_are_rejected_with_a_reason(spark, raw, reason):
    (row,) = validate(spark, [raw])
    assert row.error_reason == reason


def test_large_amount_in_weak_currency_is_valid(spark):
    # 5,000,000 XAF is only ~7,600 EUR -> valid (the limit is in EUR)
    (row,) = validate(spark, [event("e", T, amount=5_000_000, currency="XAF")])
    assert row.error_reason is None
    assert row.amount_eur == pytest.approx(7622.45, abs=0.01)


def test_schema_versions_v1_v2_and_future_v3_are_all_accepted(spark):
    v1 = event("v1", T)
    v2 = event("v2", T, schema_version=2, payment_method="card", device_id="DEV0001")
    v3 = event("v3", T, schema_version=3, payment_method="card", loyalty_points=12)  # unknown field
    no_version = without(event("v0", T), "schema_version")
    rows = {r.event_id: r for r in validate(spark, [v1, v2, v3, no_version])}

    assert all(r.error_reason is None for r in rows.values())
    assert rows["v1"].payment_method is None          # new field is NULL for v1
    assert rows["v2"].payment_method == "card"
    assert rows["v3"].payment_method == "card"        # extra field ignored, rest kept
    assert rows["v0"].schema_version == 1             # missing version = v1


def test_generated_valid_events_all_pass(spark):
    events = [generator.make_event() for _ in range(300)]
    # generator uses "now" as event_time; that's fine for the future check
    rows = validate(spark, [json.dumps(e) for e in events])
    assert [r.error_reason for r in rows if r.error_reason] == []


def test_generated_invalid_events_all_fail(spark):
    events = [generator.make_invalid_event() for _ in range(300)]
    rows = validate(spark, events)
    assert all(r.error_reason is not None for r in rows)
