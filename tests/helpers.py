"""Small helpers to build test events and Kafka-shaped DataFrames."""

import json
from datetime import datetime, timezone

# Same rates as sql/init.sql
FX_RATES = {"EUR": 1.0, "GBP": 1.17, "USD": 0.92, "NGN": 0.00058, "XAF": 0.00152449, "AED": 0.25}


def utc(text):
    """'2026-01-01 10:00:30' -> timezone-aware datetime"""
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def event(event_id, event_time, amount=10.0, currency="EUR", status="APPROVED", **extra):
    """A small valid test event; event_time like '2026-01-01T10:00:30Z'."""
    e = {
        "event_id": event_id, "schema_version": 1, "account_id": "ACC1", "merchant_id": "MER1",
        "amount": amount, "currency": currency, "country": "FR", "status": status,
        "event_time": event_time,
    }
    e.update(extra)
    return e


def kafka_df(spark, events, kafka_time="2026-01-01 10:00:00"):
    """Build a DataFrame shaped like the Kafka input (raw_value = JSON text)."""
    rows = [
        (e if isinstance(e, str) else json.dumps(e), utc(kafka_time), 0, i)
        for i, e in enumerate(events)
    ]
    return spark.createDataFrame(
        rows, "raw_value string, kafka_time timestamp, kafka_partition int, kafka_offset long"
    )
