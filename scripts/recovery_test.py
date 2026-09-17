"""
Recovery test: kill the Spark job mid-stream, start it again, and check that
no event was lost and none was counted twice.

    docker compose run --rm recovery
    docker compose run --rm recovery --rate 1000 --downtime 90

1. Sends events for 12 test products (ids 980000-980011) at --rate for the
   whole test. Only events Kafka acknowledged are counted.
2. After --warmup seconds, kills the Spark container with SIGKILL - no clean
   shutdown, the worst case.
3. Keeps sending for --downtime seconds, then starts Spark again.
4. Measures how long Spark takes to write its first new result, and how long
   until the backlog built up during the outage is cleared.
5. Sends for --after more seconds, stops, then compares per product: events
   Kafka accepted vs event_count summed over every trending window. Equal
   means nothing was lost and nothing was counted twice, even though Spark
   re-ran the batch it was killed in.

Needs the Docker socket (mounted by the `recovery` service) to stop and start
the Spark container. Test rows are deleted at the end. Results go to the
"Recovery" section of docs/BENCHMARKS.md.
"""

import argparse
import logging
import os
import random
import sys
import threading
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from kafka import KafkaProducer

from src.common import bench, config, mongo
from src.common.kafka_io import JsonValueSerializer, StringKeySerializer

logging.getLogger("kafka").setLevel(logging.WARNING)

TEST_PRODUCTS = list(range(980_000, 980_012))
HEARTBEAT_PRODUCT = 979_999
TEST_FLOOR, TEST_CEILING = 979_999, 1_000_000     # also covers load-test probes
QUERIES = ("trending", "product_pairs", "dead_letter")
DRAINED_BELOW = 500          # floor for the "caught up" threshold below
SPARK_CONTAINER = os.getenv("SPARK_CONTAINER", "affinity-spark")


def db():
    return mongo.client(config.MONGO_URI)[config.MONGO_DB]


def cleanup():
    in_range = {"$gte": TEST_FLOOR, "$lt": TEST_CEILING}
    a = db()[config.COLL_TRENDING].delete_many({"product_id": in_range})
    b = db()[config.COLL_PAIRS].delete_many(
        {"$or": [{"product_id": in_range}, {"related_product_id": in_range}]})
    return a.deleted_count + b.deleted_count


class Sender(threading.Thread):
    """
    Test traffic at a fixed rate. Every event is its own visit, so the test
    products never form pairs (a pair would be written minutes after the
    test, too late to clean up). Counts only what Kafka acknowledged.
    """

    def __init__(self, rate):
        super().__init__(daemon=True)
        self.rate = rate
        self.stop_flag = threading.Event()
        self.acked = Counter()
        self.failed = 0
        self.lock = threading.Lock()
        self.first_sent = self.last_sent = None
        self.producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP,
            value_serializer=JsonValueSerializer(),
            key_serializer=StringKeySerializer(),
            acks="all", retries=10, linger_ms=10)

    def _ok(self, product):
        def callback(_meta):
            with self.lock:
                self.acked[product] += 1
        return callback

    def _failed(self, _exc):
        with self.lock:
            self.failed += 1

    def run(self):
        sent = 0
        start = time.time()
        self.first_sent = start
        while not self.stop_flag.is_set():
            due = int((time.time() - start) * self.rate)
            while sent < due:
                product = random.choice(TEST_PRODUCTS)
                user = random.randrange(1, 5000)
                self.producer.send(config.TOPIC_EVENTS, key=user, value={
                    "event_id": uuid.uuid4().hex,
                    "session_id": uuid.uuid4().hex,
                    "user_id": user, "product_id": product,
                    "event_type": "view", "timestamp": time.time(),
                }).add_callback(self._ok(product)).add_errback(self._failed)
                sent += 1
            self.stop_flag.wait(0.01)
        self.last_sent = time.time()
        self.producer.flush()

    def stop(self):
        self.stop_flag.set()
        self.join(timeout=60)

    def heartbeat(self, stop):
        """Keeps event time moving while the final counts settle."""
        while not stop.is_set():
            self.producer.send(config.TOPIC_EVENTS, key=0, value={
                "event_id": uuid.uuid4().hex, "session_id": uuid.uuid4().hex,
                "user_id": 1, "product_id": HEARTBEAT_PRODUCT,
                "event_type": "view", "timestamp": time.time()})
            stop.wait(2)


def counted():
    """{product: event_count summed over all its trending windows}."""
    rows = db()[config.COLL_TRENDING].aggregate([
        {"$match": {"product_id": {"$in": TEST_PRODUCTS}}},
        {"$group": {"_id": "$product_id", "events": {"$sum": "$event_count"}}},
    ])
    return {r["_id"]: int(r["events"]) for r in rows}


def last_test_write():
    doc = db()[config.COLL_TRENDING].find_one(
        {"product_id": {"$in": TEST_PRODUCTS}}, sort=[("_updated_at", -1)])
    return bench.to_epoch(doc["_updated_at"]) if doc else None


def progress_since(since):
    lo = datetime.fromtimestamp(since, timezone.utc)
    return list(db()["pipeline_metrics"].find(
        {"recorded_at": {"$gt": lo}}).sort("recorded_at", 1))


def lag_of(doc):
    values = [v for v in (doc.get("kafka_lag") or []) if v is not None]
    return max(values) if values else None


def fmt_s(value):
    return "-" if value is None else f"{value:.0f} s"


def fmt_n(value):
    return "-" if value is None else f"{value:,.0f}"


def main():
    sys.stdout.reconfigure(line_buffering=True)
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--rate", type=int, default=500,
                        help="test events per second (default 500)")
    parser.add_argument("--warmup", type=int, default=60)
    parser.add_argument("--downtime", type=int, default=60,
                        help="seconds Spark stays down (default 60)")
    parser.add_argument("--after", type=int, default=120,
                        help="seconds of traffic after the restart")
    parser.add_argument("--settle", type=int, default=300,
                        help="max seconds to wait for the final counts")
    args = parser.parse_args()

    try:
        ok = requests.get(f"{config.API_BASE_URL}/health", timeout=10).json()
    except requests.RequestException:
        ok = None
    if not ok or ok.get("status") != "ok":
        print("API not healthy. Start the stack first: docker compose up -d")
        return 1
    docker = bench.DockerEngine()
    try:
        if not docker.is_running(SPARK_CONTAINER):
            print(f"{SPARK_CONTAINER} is not running. Start it first.")
            return 1
    except (OSError, RuntimeError) as exc:
        print(f"Cannot reach Docker through /var/run/docker.sock: {exc}\n"
              "Run this through `docker compose run --rm recovery`.")
        return 1

    removed = cleanup()
    if removed:
        print(f"Removed {removed} test rows left by an earlier run.")
    env = bench.environment()
    print(f"Test: {args.rate} events/s | warm-up {args.warmup}s | "
          f"Spark down {args.downtime}s | {args.after}s after restart\n")

    sender = Sender(args.rate)
    sender.start()
    results = {}
    try:
        print("Warming up...")
        if not bench.wait_until(last_test_write, timeout=args.warmup + 60):
            print("FAIL  Spark wrote nothing for the test products during "
                  "warm-up.")
            return 1
        remaining = args.warmup - (time.time() - sender.first_sent)
        if remaining > 0:
            time.sleep(remaining)

        docker.kill(SPARK_CONTAINER)
        killed_at = time.time()
        print(f"Killed {SPARK_CONTAINER} (SIGKILL).")
        time.sleep(3)
        if docker.is_running(SPARK_CONTAINER):
            print("  Docker restarted it straight away (restart policy).")
        else:
            time.sleep(max(args.downtime - 3, 0))
            docker.start(SPARK_CONTAINER)
        restarted_at = docker.started_at(SPARK_CONTAINER)
        print(f"Spark started again {restarted_at - killed_at:.0f}s after "
              "the kill. Measuring recovery...")

        first_write = bench.wait_until(
            lambda: (lambda t: t if t and t > restarted_at else None)(
                last_test_write()),
            timeout=300, interval=1)
        # The events that piled up while Spark was down are read by the first
        # batch after the restart (batches are not size-capped).
        backlog = {"rows": None}
        drained = {}
        # Traffic keeps flowing, so a query that has caught up still has
        # (rate x batch time) events unread after each batch. Caught up means
        # less than one trigger interval's worth of events is waiting.
        trigger = bench.seconds_in(config.TRIGGER_INTERVAL) or 10.0
        caught_up_below = max(DRAINED_BELOW, (args.rate + 50) * trigger)

        def caught_up():
            for doc in progress_since(restarted_at):
                q, lag = doc.get("query"), lag_of(doc)
                if q == "trending" and backlog["rows"] is None \
                        and doc.get("input_rows"):
                    backlog["rows"] = doc["input_rows"]
                if q in QUERIES and q not in drained and lag is not None \
                        and lag <= caught_up_below:
                    drained[q] = bench.to_epoch(doc["recorded_at"])
            return len(drained) == len(QUERIES)

        cleared = bench.wait_until(caught_up, timeout=300, interval=3)
        results = {
            "downtime": restarted_at - killed_at,
            "first_write": first_write and first_write - restarted_at,
            "caught_up": (max(drained.values()) - restarted_at) if cleared else None,
            "backlog": backlog["rows"],
        }
        print(f"  first result {fmt_s(results['first_write'])} after the "
              f"restart; first batch read {fmt_n(results['backlog'])} events; "
              f"backlog cleared {fmt_s(results['caught_up'])} after the restart")

        remaining = restarted_at + args.after - time.time()
        if remaining > 0:
            time.sleep(remaining)
    finally:
        sender.stop()
        try:
            if not docker.is_running(SPARK_CONTAINER):
                docker.start(SPARK_CONTAINER)
                print(f"Started {SPARK_CONTAINER} again.")
        except (OSError, RuntimeError) as exc:
            print(f"WARNING: could not check {SPARK_CONTAINER}: {exc}")

    expected = dict(sender.acked)
    print(f"\nStopped sending: {sum(expected.values()):,} events acknowledged "
          f"by Kafka, {sender.failed} rejected. Waiting for the final counts...")
    stop = threading.Event()
    beat = threading.Thread(target=sender.heartbeat, args=(stop,), daemon=True)
    beat.start()
    final = bench.wait_until(
        lambda: (lambda c: c if c == expected else None)(counted()),
        timeout=args.settle, interval=5) or counted()
    stop.set()
    beat.join(timeout=5)

    windows = db()[config.COLL_TRENDING].aggregate([
        {"$match": {"product_id": {"$in": TEST_PRODUCTS}}},
        {"$group": {"_id": {"w": "$window_start", "p": "$product_id"},
                    "n": {"$sum": 1}}},
        {"$match": {"n": {"$gt": 1}}},
    ])
    duplicate_rows = len(list(windows))
    minutes = sorted({doc["window_start"] for doc in db()[config.COLL_TRENDING]
                      .find({"product_id": {"$in": TEST_PRODUCTS}},
                            {"window_start": 1})})
    gaps = sum(1 for a, b in zip(minutes, minutes[1:], strict=False)
               if (b - a).total_seconds() > 60)
    removed = cleanup()
    sender.producer.close()

    sent_total = sum(expected.values())
    counted_total = sum(final.values())
    lost = sum(max(expected.get(p, 0) - final.get(p, 0), 0) for p in TEST_PRODUCTS)
    extra = sum(max(final.get(p, 0) - expected.get(p, 0), 0) for p in TEST_PRODUCTS)
    exact = final == expected and duplicate_rows == 0 and gaps == 0

    print(f"\n{'PASS' if final == expected else 'FAIL'}  every acknowledged "
          f"event counted exactly once ({counted_total:,} counted, "
          f"{lost} missing, {extra} extra)")
    print(f"{'PASS' if duplicate_rows == 0 else 'FAIL'}  no window stored "
          f"twice ({duplicate_rows} duplicates)")
    print(f"{'PASS' if gaps == 0 else 'FAIL'}  no missing minute across the "
          f"outage ({len(minutes)} windows, {gaps} gaps)")
    print(f"\nRemoved {removed} test rows.")

    body = "\n".join([
        "## Recovery after a crash",
        "",
        f"Measured {env['measured_at']} with `docker compose run --rm "
        f"recovery`: {args.rate:,} test events/s, Spark killed with SIGKILL "
        f"after {args.warmup} s and started again {results.get('downtime', 0):.0f}"
        " s later, traffic continuing throughout.",
        "",
        "| | |",
        "|---|---|",
        f"| Events read by the first batch after the restart (the backlog) | "
        f"{fmt_n(results.get('backlog'))} |",
        f"| First new result after the restart | "
        f"{fmt_s(results.get('first_write'))} |",
        f"| Backlog cleared after the restart | "
        f"{fmt_s(results.get('caught_up'))} |",
        f"| Events acknowledged by Kafka | {sent_total:,} |",
        f"| Events counted in MongoDB | {counted_total:,} "
        f"({lost} missing, {extra} extra) |",
        f"| Windows stored twice | {duplicate_rows} |",
        f"| Minutes missing across the outage | {gaps} |",
        "",
        ("**Exactly-once results:** every event was counted once. A batch "
         "interrupted by the kill is run again after the restart: Spark "
         "resumes from the Kafka offsets and state in its checkpoint, and the "
         "MongoDB writes are upserts on the window key, so the re-run "
         "overwrites instead of adding (ADR 0004)."
         if exact else
         "**The counts did not match** - see the table and the script output."),
        "",
        "The first result includes the JVM and Spark start-up; the backlog "
        "is cleared in the first batch or two after that.",
    ])
    bench.update_section("recovery", body)
    print(f"Wrote the Recovery section of {bench.REPORT_PATH}.")
    return 0 if exact else 1


if __name__ == "__main__":
    sys.exit(main())
