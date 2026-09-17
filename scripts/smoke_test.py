"""
End-to-end check against a running stack.

Produces a burst of sessions with a known co-view pattern, waits for the
pipeline to close a window, then asserts the API actually returns the pair it
should. This is the difference between "the containers are up" and "the
pipeline works".

    python scripts/smoke_test.py

Needs the stack and the API running. Takes up to ~9 minutes.
"""

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from kafka import KafkaProducer

from src.common import catalog, config
from src.common.kafka_io import JsonValueSerializer, StringKeySerializer

# A pair that must co-occur if the pipeline works: laptop + laptop sleeve.
ANCHOR = 9001
PARTNER = 9003
SESSIONS = 60
# Worst case: a 5-minute co-occurrence window that has just opened, plus the
# 2-minute watermark, plus a trigger. 240 s was not always enough.
WAIT_SECONDS = 540
POLL_SECONDS = 10
# Heartbeat events keep the watermark moving when no producer is running.
# Each gets its own session, so it can never form a pair.
HEARTBEAT_USER = catalog.USERS[-1]
HEARTBEAT_PRODUCT = 9011

FAILURES: list = []


def check(name, ok, detail=""):
    print(("PASS  " if ok else "FAIL  ") + name + ("" if ok else f": {detail}"))
    if not ok:
        FAILURES.append(name)


def api(path):
    return requests.get(f"{config.API_BASE_URL}{path}", timeout=10).json()


def main():
    sys.stdout.reconfigure(line_buffering=True)   # show progress live
    print(f"Kafka   : {config.KAFKA_BOOTSTRAP}")
    print(f"API     : {config.API_BASE_URL}")
    print()

    try:
        health = api("/health")
        check("API is reachable and Mongo is up",
              health.get("status") == "ok", json.dumps(health))
    except Exception as exc:                                  # noqa: BLE001
        check("API is reachable", False, str(exc))
        print("\nStart the stack first: docker compose up -d --build")
        return 1

    dlq_before = api("/stats")["collections"][config.COLL_DLQ]

    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        value_serializer=JsonValueSerializer(),
        key_serializer=StringKeySerializer(),
    )
    print(f"Producing {SESSIONS} sessions of the pair "
          f"{catalog.name_of(ANCHOR)} + {catalog.name_of(PARTNER)}...")
    for i in range(SESSIONS):
        user = catalog.USERS[i % len(catalog.USERS)]
        now = time.time()
        for pid, etype, offset in ((ANCHOR, "view", 0.0),
                                   (PARTNER, "add_to_cart", 1.0)):
            producer.send(config.TOPIC_EVENTS, key=user, value={
                "event_id": f"smoke-{i}-{pid}", "user_id": user,
                "product_id": pid, "event_type": etype,
                "timestamp": now + offset})
    producer.send(config.TOPIC_EVENTS, key=ANCHOR, value=b"{smoke: not json")
    producer.flush()
    check("events accepted by Kafka", True)

    print(f"\nWaiting up to {WAIT_SECONDS}s for the windows to close...")
    print("A window closes only when newer events push the watermark past it,")
    print(f"so a heartbeat event is sent every {POLL_SECONDS}s.")
    deadline = time.time() + WAIT_SECONDS
    trending_ok = recs = None
    while time.time() < deadline:
        time.sleep(POLL_SECONDS)
        beat = time.time()
        producer.send(config.TOPIC_EVENTS, key=HEARTBEAT_USER, value={
            "event_id": f"smoke-heartbeat-{beat}",
            "session_id": f"smoke-heartbeat-{beat}",
            "user_id": HEARTBEAT_USER, "product_id": HEARTBEAT_PRODUCT,
            "event_type": "view", "timestamp": beat})
        producer.flush()
        try:
            t = api("/trending?limit=10")
            if t.get("trending"):
                trending_ok = t
            r = api(f"/related-products/{ANCHOR}?limit=10")
            if r.get("source") == "co_occurrence":
                recs = r
                break
        except Exception:                                     # noqa: BLE001
            continue
        print(f"  ...still waiting ({int(deadline - time.time())}s left)")
    producer.close()

    check("trending window produced results", bool(trending_ok),
          "no trending rows within the wait window")
    if trending_ok:
        ids = [row["product_id"] for row in trending_ok["trending"]]
        check("the products we produced appear in trending",
              ANCHOR in ids or PARTNER in ids, str(ids))
        scores = [row["score"] for row in trending_ok["trending"]]
        check("trending is sorted by score descending",
              scores == sorted(scores, reverse=True), str(scores))

    check("related products came from co-occurrence, not the fallback",
          bool(recs), "still returning trending_fallback after the wait")
    if recs:
        related = [row["product_id"] for row in recs["related_products"]]
        check("the co-viewed partner is listed as related",
              PARTNER in related, str(related))
        check("a product is never related to itself",
              ANCHOR not in related, str(related))
        check("related products carry catalogue names",
              all(row["name"] for row in recs["related_products"]))

    stats = api("/stats")
    print("\nPipeline state:", json.dumps(stats.get("collections", {})))
    dlq_after = stats["collections"][config.COLL_DLQ]
    check("dead-letter queue captured the malformed smoke event",
          dlq_after > dlq_before, f"before={dlq_before} after={dlq_after}")

    print()
    print("ALL PASSED" if not FAILURES else f"{len(FAILURES)} FAILED: {FAILURES}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
