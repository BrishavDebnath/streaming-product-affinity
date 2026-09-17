"""
Clickstream event producer.

Unlike a uniform random generator, this emits SESSIONS: a shopper picks a
product, then browses a few more from related categories. That matters,
because uniform random events make every product pair equally likely and the
co-occurrence pipeline has nothing real to find. With sessions, laptops
genuinely co-occur with laptop accessories, and you can tell at a glance
whether the pairing works.

    python -m src.producer.producer
    EVENTS_PER_SECOND=100 python -m src.producer.producer
    python -m src.producer.producer --total 5000     # finite run, prints rate
"""

import argparse
import logging
import random
import signal
import sys
import time
import uuid

from kafka import KafkaProducer

from src.common import catalog, config
from src.common.kafka_io import JsonValueSerializer, StringKeySerializer

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s producer | %(message)s")
log = logging.getLogger("producer")

_RUNNING = True


def _stop(signum, frame):
    global _RUNNING
    _RUNNING = False
    log.info("signal %s received, shutting down", signum)


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        # Accepts a dict (JSON-encoded) or raw bytes (the deliberately
        # malformed events). Without the bytes passthrough the malformed
        # branch had to build a SECOND KafkaProducer per event and never
        # closed it - an endless bootstrap/connect/"Closing transport" cycle
        # in the logs, leaking a socket and threads every time.
        value_serializer=JsonValueSerializer(),
        # Partitioning by user_id keeps one shopper's events on one partition,
        # so their ordering is preserved end to end.
        key_serializer=StringKeySerializer(),
        acks="all",
        retries=5,
        linger_ms=20,
    )


def make_session(user_id: int):
    """One shopper's burst of related events."""
    n = random.randint(config.SESSION_MIN_EVENTS, config.SESSION_MAX_EVENTS)
    chosen = catalog.session_products(n, config.CROSS_CATEGORY_RATE)

    # One id per browsing session. The co-occurrence join keys on this, so two
    # products pair only when the SAME shopper saw both in the SAME visit.
    # Keying on user_id alone pairs every event a continuously-active user
    # makes with every other one: at 20 events/s across 50 users that is
    # ~1.3M pairs per window instead of ~18k, and the counts stop measuring
    # affinity and start measuring popularity.
    session_id = str(uuid.uuid4())

    # A whole session uses one wire format - a real producer instance runs one
    # build, it does not switch versions mid-session.
    use_v2 = random.random() < config.SCHEMA_V2_RATIO
    channel = random.choice(config.CHANNELS)

    events = []
    for product in chosen:
        etype = random.choices(
            population=["view", "click", "add_to_cart", "purchase", "search"],
            weights=[55, 22, 13, 4, 6],
            k=1,
        )[0]
        base = {
            "event_id": str(uuid.uuid4()),
            "session_id": session_id,
            "user_id": user_id,
            "product_id": product["id"],
            "timestamp": time.time(),
        }
        if use_v2:
            base.update({"schema_version": 2, "action": etype,
                         "channel": channel})
        else:
            base["event_type"] = etype        # v1 carries no schema_version
        events.append(base)
    return events


def make_malformed(user_id: int):
    """Deliberately broken events, so the dead-letter path is exercised."""
    kind = random.choice(["bad_type", "null_field", "not_json"])
    if kind == "not_json":
        return "{definitely not json"
    if kind == "bad_type":
        return {"event_id": str(uuid.uuid4()),
                "session_id": str(uuid.uuid4()), "user_id": user_id,
                "product_id": random.choice(catalog.product_ids()),
                "event_type": "teleport", "timestamp": time.time()}
    return {"event_id": str(uuid.uuid4()),
            "session_id": str(uuid.uuid4()), "user_id": None,
            "product_id": random.choice(catalog.product_ids()),
            "event_type": "view", "timestamp": time.time()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--total", type=int, default=0,
                        help="stop after N events (0 = run until interrupted)")
    parser.add_argument("--rate", type=float, default=config.EVENTS_PER_SECOND)
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    producer = build_producer()
    log.info("connected to %s, topic=%s, target rate=%.1f events/s, "
             "v2 schema ratio=%.0f%%",
             config.KAFKA_BOOTSTRAP, config.TOPIC_EVENTS, args.rate,
             config.SCHEMA_V2_RATIO * 100)

    delay = 1.0 / args.rate
    sent = malformed = 0
    started = time.time()

    while _RUNNING and (args.total == 0 or sent < args.total):
        user_id = random.choice(catalog.USERS)

        if random.random() < config.MALFORMED_RATE:
            payload = make_malformed(user_id)
            # str -> bytes so the parser sees genuinely invalid JSON; the
            # serializer passes bytes through. One producer, reused.
            if isinstance(payload, str):
                payload = payload.encode("utf-8")
            producer.send(config.TOPIC_EVENTS, key=user_id, value=payload)
            malformed += 1
            sent += 1
            time.sleep(delay)
            continue

        for ev in make_session(user_id):
            if not _RUNNING or (args.total and sent >= args.total):
                break
            producer.send(config.TOPIC_EVENTS, key=user_id, value=ev)
            sent += 1
            if sent % 500 == 0:
                elapsed = time.time() - started
                log.info("sent=%s malformed=%s rate=%.1f/s",
                         sent, malformed, sent / max(elapsed, 1e-6))
            time.sleep(delay)

    producer.flush()
    producer.close()
    elapsed = time.time() - started
    log.info("done: %s events (%s malformed) in %.1fs -> %.1f events/s",
             sent, malformed, elapsed, sent / max(elapsed, 1e-6))
    return 0


if __name__ == "__main__":
    sys.exit(main())
