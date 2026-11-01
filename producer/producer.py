"""
Synthetic payment event producer: sends events from generator.py to Kafka.

On top of the generator's messy data it also simulates two transport problems:
  * duplicates  - the same message sent twice (same event_id)
  * malformed   - messages that are not valid JSON
"""

import json
import logging
import os
import random
import time

from confluent_kafka import Producer

from generator import next_events

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("TOPIC", "payments")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "5"))

DUPLICATE_RATE = 0.03
MALFORMED_RATE = 0.005

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s producer: %(message)s")
log = logging.getLogger(__name__)


def make_producer(bootstrap=KAFKA_BOOTSTRAP):
    """Create a Kafka producer with safe delivery settings."""
    # Delivery guarantee on the producer side:
    #   acks=all             -> broker confirms only when the write is safe
    #   enable.idempotence   -> retries never create duplicates inside Kafka
    return Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 50,
    })


def send(producer, event):
    """Send one event, keyed by account_id so an account's events stay in order."""
    key = event.get("account_id", "unknown").encode("utf-8")
    producer.produce(TOPIC, key=key, value=json.dumps(event).encode("utf-8"), on_delivery=delivery_report)


def delivery_report(err, msg):
    """Called by Kafka for every message: log it if delivery failed."""
    if err is not None:
        log.error("Delivery failed: %s", err)


def main():
    """Send events forever at about EVENTS_PER_SECOND."""
    producer = make_producer()
    log.info("Sending ~%s events/sec to topic '%s' on %s", EVENTS_PER_SECOND, TOPIC, KAFKA_BOOTSTRAP)
    sent = 0
    while True:
        if random.random() < MALFORMED_RATE:
            producer.produce(TOPIC, value=b'{"event_id": "broken", "amount": ', on_delivery=delivery_report)

        for event in next_events():
            send(producer, event)
            # Simulate an upstream system that re-sends the same message
            if random.random() < DUPLICATE_RATE:
                send(producer, event)
            sent += 1

        producer.poll(0)
        if sent % 500 == 0:
            log.info("%d events sent", sent)
        time.sleep(1 / EVENTS_PER_SECOND)


if __name__ == "__main__":
    main()
