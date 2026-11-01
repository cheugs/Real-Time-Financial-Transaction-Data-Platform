"""Unit tests for the synthetic event generator (plain Python, no Spark)."""

from datetime import datetime, timedelta, timezone

import generator as g


def parse(ts):
    """Parse the generator's ISO timestamp."""
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def test_every_event_has_a_unique_id():
    ids = [g.make_event()["event_id"] for _ in range(5000)]
    assert len(ids) == len(set(ids))


def test_events_have_valid_financial_attributes():
    for _ in range(1000):
        e = g.make_event()
        assert e["amount"] > 0
        assert e["currency"] in g.APPROX_EUR_RATE
        assert e["status"] in g.STATUSES
        assert e["account_id"] in g.ACCOUNTS
        # currency matches the country
        assert e["currency"] == g.CURRENCY.get(e["country"], "EUR")
        # event_time is valid ISO-8601 UTC and close to now
        assert abs(parse(e["event_time"]) - datetime.now(timezone.utc)) < timedelta(seconds=5)


def test_schema_v1_has_no_new_fields_and_v2_has_them():
    v1 = g.make_event(schema_version=1)
    v2 = g.make_event(schema_version=2)
    assert "payment_method" not in v1 and "device_id" not in v1
    assert v2["payment_method"] in g.PAYMENT_METHODS and v2["device_id"].startswith("DEV")


def test_amounts_are_realistic_per_currency():
    # the same ~100 EUR payment is a big number in NGN and a smaller one in GBP
    ngn = g.make_event(country="NG", amount_eur=100)
    gbp = g.make_event(country="GB", amount_eur=100)
    assert ngn["amount"] > 100_000
    assert 80 < gbp["amount"] < 90


def test_iso_format():
    ts = datetime(2026, 1, 2, 3, 4, 5, 678000, tzinfo=timezone.utc)
    assert g.iso(ts) == "2026-01-02T03:04:05.678Z"


def test_invalid_events_break_a_rule():
    for _ in range(300):
        e = g.make_invalid_event()
        broken = (
            e.get("amount", 1) <= 0
            or "account_id" not in e
            or e["event_time"] == "not-a-date"
            or parse(e["event_time"]) > datetime.now(timezone.utc) + timedelta(hours=1)
            or e["currency"] not in g.APPROX_EUR_RATE
            or e["status"] not in g.STATUSES
        )
        assert broken, e


def test_late_events_are_generated(monkeypatch):
    monkeypatch.setattr(g, "VERY_LATE_RATE", 0)
    monkeypatch.setattr(g, "LATE_RATE", 1.0)
    monkeypatch.setattr(g, "BURST_RATE", 0)
    monkeypatch.setattr(g, "LARGE_AMOUNT_RATE", 0)
    monkeypatch.setattr(g, "COUNTRY_HOP_RATE", 0)
    monkeypatch.setattr(g, "INVALID_RATE", 0)
    (e,) = g.next_events()
    delay = datetime.now(timezone.utc) - parse(e["event_time"])
    assert timedelta(seconds=29) < delay < timedelta(seconds=301)


def test_burst_pattern_uses_one_account(monkeypatch):
    monkeypatch.setattr(g, "BURST_RATE", 1.0)
    events = g.next_events()
    assert len(events) >= 8
    assert len({e["account_id"] for e in events}) == 1
