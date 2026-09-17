"""
Backfill synthetic history so the dashboard has data the moment it opens.

Without this, a fresh clone shows empty panels for several minutes: trending
needs a window to close, and co-occurrence needs its window to close AND newer
events to push the watermark past it. Anyone evaluating the project sees a blank page
and assumes it is broken.

This writes events with timestamps spread across the recent PAST. Because the
event times are already old, the watermark jumps forward as soon as they are
consumed and every window they belong to closes immediately - so results appear
in one trigger interval instead of several minutes.

    python scripts/seed.py                  # 20 minutes of history
    python scripts/seed.py --minutes 60 --sessions 800

Run it on a fresh stack BEFORE starting the producer. Once live events have
moved the watermark forward, these back-dated events are dropped as late.
"""

import argparse
import os
import random
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer

from src.common import catalog, config
from src.common.kafka_io import JsonValueSerializer, StringKeySerializer


def build_producer():
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        value_serializer=JsonValueSerializer(),
        key_serializer=StringKeySerializer(),
        acks="all", linger_ms=20,
    )


def make_session(user_id, at_time, legacy):
    """One shopper's visit, timestamped in the past."""
    n = random.randint(config.SESSION_MIN_EVENTS, config.SESSION_MAX_EVENTS)
    chosen = catalog.session_products(n, config.CROSS_CATEGORY_RATE)

    session_id = str(uuid.uuid4())
    events = []
    for offset, product in enumerate(chosen):
        etype = random.choices(
            ["view", "click", "add_to_cart", "purchase", "search"],
            weights=[55, 22, 13, 4, 6], k=1)[0]
        stamp = at_time + offset * random.uniform(2, 20)
        if legacy:                      # old producer: no v2 fields
            # v1 producers already sent session_id. Leaving it out made the
            # job pair these events by shopper ACROSS visits - the source of
            # the weak cross-category pairs on the graph.
            events.append({"event_id": str(uuid.uuid4()),
                           "session_id": session_id, "user_id": user_id,
                           "product_id": product["id"], "event_type": etype,
                           "timestamp": stamp})
        else:
            events.append({"schema_version": config.SCHEMA_VERSION,
                           "event_id": str(uuid.uuid4()),
                           "session_id": session_id,
                           "channel": random.choice(["web", "android", "ios"]),
                           "user_id": user_id, "product_id": product["id"],
                           "event_type": etype, "timestamp": stamp})
    return events


def already_has_results():
    """True when the trending collection has any rows."""
    from pymongo import MongoClient
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=10000)
    try:
        coll = client[config.MONGO_DB][config.COLL_TRENDING]
        return coll.estimated_document_count() > 0
    finally:
        client.close()


def main():
    sys.stdout.reconfigure(line_buffering=True)   # show progress live
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=int, default=20,
                        help="how far back to backfill")
    parser.add_argument("--sessions", type=int, default=400)
    parser.add_argument("--malformed", type=int, default=5,
                        help="malformed events, to populate the DLQ")
    parser.add_argument("--if-empty", action="store_true",
                        help="do nothing if MongoDB already holds results "
                             "(used by docker compose, which runs this on "
                             "every start)")
    args = parser.parse_args()

    if args.if_empty and already_has_results():
        print("MongoDB already has results - skipping the backfill.")
        return 0

    now = time.time()
    start = now - args.minutes * 60
    producer = build_producer()
    print(f"Seeding {args.sessions} sessions across the last "
          f"{args.minutes} minutes -> {config.KAFKA_BOOTSTRAP}")

    sent = 0
    for i in range(args.sessions):
        # Spread sessions over the window, leaving the last 60 s clear so the
        # final window is still open and the watermark keeps advancing.
        at_time = start + (i / args.sessions) * (args.minutes * 60 - 60)
        legacy = random.random() < config.LEGACY_EVENT_RATE
        user = random.choice(catalog.USERS)
        for ev in make_session(user, at_time, legacy):
            producer.send(config.TOPIC_EVENTS, key=user, value=ev)
            sent += 1
        if (i + 1) % 100 == 0:
            print(f"  {i + 1}/{args.sessions} sessions, {sent:,} events")

    for _ in range(args.malformed):
        producer.send(config.TOPIC_EVENTS, key=catalog.USERS[0],
                      value=b"{ not json at all")
        sent += 1

    producer.flush()
    producer.close()
    print(f"\nSeeded {sent:,} events ({args.malformed} malformed).")
    print("Windows should close within one trigger interval "
          f"({config.TRIGGER_INTERVAL}) because the event times are in the past.")
    print("\nOpen the dashboard at http://localhost:8501")
    return 0


if __name__ == "__main__":
    sys.exit(main())
