"""
Builds synthetic payment events (pure Python, no Kafka here so it is easy to test).

Besides normal payments it can create "messy" data on purpose:

  * late events      - event_time a few minutes in the past
  * very late events - event_time older than the Spark watermark (20-40 min)
  * invalid events   - negative amount, unknown currency, bad timestamp, ...
  * schema v1 / v2   - v2 adds two optional fields (payment_method, device_id)

And simple "suspicious" patterns (bursts, large amounts, country hopping).
These are SIMULATED patterns, not real fraud.
"""

import random
import uuid
from datetime import datetime, timedelta, timezone

# Probabilities for the messy data
LATE_RATE = 0.05
VERY_LATE_RATE = 0.01
INVALID_RATE = 0.01
SCHEMA_V2_RATE = 0.5

# Probabilities for the simulated suspicious patterns
BURST_RATE = 0.005
LARGE_AMOUNT_RATE = 0.01
COUNTRY_HOP_RATE = 0.003

COUNTRIES = ["GB", "FR", "DE", "ES", "IT", "NL", "US", "NG", "CM", "AE"]
CURRENCY = {"GB": "GBP", "US": "USD", "NG": "NGN", "CM": "XAF", "AE": "AED"}   # others use EUR
PAYMENT_METHODS = ["card", "apple_pay", "google_pay", "bank_transfer"]
STATUSES = ["APPROVED", "DECLINED", "PENDING"]
STATUS_WEIGHTS = [0.92, 0.06, 0.02]

# Approximate value of 1 unit of each currency in EUR. Only used to make the
# amounts look realistic (e.g. NGN amounts are big numbers). The pipeline uses
# its own fx_rates table in Postgres for the conversion.
APPROX_EUR_RATE = {"EUR": 1.0, "GBP": 1.17, "USD": 0.92, "NGN": 0.00058, "XAF": 0.001524, "AED": 0.25}

# 2000 accounts at 5 events/sec -> a normal account makes < 1 payment per 5 minutes,
# so only the injected bursts trigger the "high velocity" rule.
ACCOUNTS = [f"ACC{i:04d}" for i in range(2000)]
_rng = random.Random(42)   # fixed seed -> every run uses the same home countries
HOME_COUNTRY = {acc: _rng.choice(COUNTRIES) for acc in ACCOUNTS}
MERCHANTS = [f"MER{i:03d}" for i in range(50)]


def now_utc():
    """Current time in UTC."""
    return datetime.now(timezone.utc)


def iso(ts):
    """Format a datetime as ISO-8601 UTC, e.g. 2026-11-01T12:00:00.123Z"""
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def make_event(account_id=None, amount_eur=None, country=None, event_time=None, schema_version=None):
    """Build one valid payment event as a Python dict."""
    account_id = account_id or random.choice(ACCOUNTS)
    country = country or HOME_COUNTRY.get(account_id, "FR")
    currency = CURRENCY.get(country, "EUR")
    if amount_eur is None:
        # most payments are small, a few are bigger
        amount_eur = random.lognormvariate(3.5, 0.8)
    # convert to the local currency so the number looks realistic
    amount = round(amount_eur / APPROX_EUR_RATE[currency], 2)
    event_time = event_time or now_utc()
    if schema_version is None:
        schema_version = 2 if random.random() < SCHEMA_V2_RATE else 1

    event = {
        "event_id": str(uuid.uuid4()),
        "schema_version": schema_version,
        "account_id": account_id,
        "merchant_id": random.choice(MERCHANTS),
        "amount": max(amount, 0.01),
        "currency": currency,
        "country": country,
        "status": random.choices(STATUSES, STATUS_WEIGHTS)[0],
        "event_time": iso(event_time),
    }

    # Schema evolution: version 2 adds two optional fields.
    if schema_version == 2:
        event["payment_method"] = random.choice(PAYMENT_METHODS)
        event["device_id"] = f"DEV{random.randint(1, 500):04d}"

    return event


def make_invalid_event():
    """A JSON event that breaks one data-quality rule (Spark must reject it)."""
    event = make_event()
    problem = random.choice([
        "negative_amount", "zero_amount", "missing_account", "bad_timestamp",
        "future_timestamp", "unknown_currency", "bad_status",
    ])
    if problem == "negative_amount":
        event["amount"] = -abs(event["amount"])
    elif problem == "zero_amount":
        event["amount"] = 0
    elif problem == "missing_account":
        del event["account_id"]
    elif problem == "bad_timestamp":
        event["event_time"] = "not-a-date"
    elif problem == "future_timestamp":
        event["event_time"] = iso(now_utc() + timedelta(days=1))
    elif problem == "unknown_currency":
        event["currency"] = "XXX"
    elif problem == "bad_status":
        event["status"] = "MAYBE"
    return event


def next_events():
    """Return the list of events to send in this step (usually just one)."""
    r = random.random()

    # --- simulated suspicious patterns ---
    if r < BURST_RATE:
        # one account makes many payments in a few seconds
        acc = random.choice(ACCOUNTS)
        return [make_event(account_id=acc) for _ in range(random.randint(8, 12))]

    if r < BURST_RATE + LARGE_AMOUNT_RATE:
        return [make_event(amount_eur=random.uniform(3000, 9000))]

    if r < BURST_RATE + LARGE_AMOUNT_RATE + COUNTRY_HOP_RATE:
        # same account, several countries within a short time
        acc = random.choice(ACCOUNTS)
        return [make_event(account_id=acc, country=c) for c in random.sample(COUNTRIES, 4)]

    if r < BURST_RATE + LARGE_AMOUNT_RATE + COUNTRY_HOP_RATE + INVALID_RATE:
        return [make_invalid_event()]

    # --- normal event, possibly late ---
    r = random.random()
    if r < VERY_LATE_RATE:
        t = now_utc() - timedelta(minutes=random.uniform(20, 40))
    elif r < VERY_LATE_RATE + LATE_RATE:
        t = now_utc() - timedelta(seconds=random.uniform(30, 300))
    else:
        t = now_utc()
    return [make_event(event_time=t)]
