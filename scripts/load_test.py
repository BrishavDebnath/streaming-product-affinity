"""
Load test: which event rates the pipeline keeps up with, and how long an event
takes to reach MongoDB.

    docker compose run --rm loadtest
    docker compose run --rm loadtest --rates 1000 5000 --seconds 60

For each target rate, several producer processes send realistic sessions
(the live producer's own generator) for --seconds. Meanwhile probe events with
unique product ids measure end-to-end latency, Kafka -> Spark -> MongoDB:

  trending probe   one event; latency = when its trending row is written
  pair probe       two events in one visit; latency = when the pair is written.
                   A pair can only be final once its window, the co-view gap
                   and the watermark have all passed (about 5 minutes).

After each step the script waits for Spark to clear the backlog, then reads
what the Spark job itself recorded during the step (pipeline_metrics).

Results: results/load_test.csv and the "Throughput" section of
docs/BENCHMARKS.md. Probe rows are deleted from MongoDB at the end.
Keep the live producer running: it moves event time forward.
"""

import argparse
import csv
import logging
import multiprocessing as mp
import os
import random
import sys
import threading
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from kafka import KafkaProducer

from src.common import bench, config, kafka_io, mongo
from src.common.kafka_io import JsonValueSerializer, StringKeySerializer
from src.producer.producer import make_session

# kafka-python logs every connection step at INFO; only problems matter here.
logging.getLogger("kafka").setLevel(logging.WARNING)

DEFAULT_RATES = [2500, 5000, 10000, 15000, 20000]
QUERIES = ("trending", "product_pairs", "dead_letter")

# Probe product ids live far outside the catalogue (9001-9012) and are deleted
# afterwards, so they never show up on the dashboard for long.
PROBE_FLOOR = 989_999
HEARTBEAT_PRODUCT = 989_999
TREND_PROBE_BASE = 990_000
PAIR_PROBE_BASE = 995_000
PROBE_USER = 999_999
LOAD_USERS = 20_000            # enough distinct keys to use every partition
DRAINED_BELOW = 500            # events left unread that count as "caught up"
WARMUP_TRIGGERS = 3            # batches ignored at the start of each step


def make_producer():
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        value_serializer=JsonValueSerializer(),
        key_serializer=StringKeySerializer(),
        acks=1,                     # a throughput test, not a durability test
        # Idempotence needs acks=all; saying so explicitly stops kafka-python
        # printing a warning for every producer process.
        enable_idempotence=False,
        linger_ms=20,
        batch_size=256 * 1024,
    )


# ----------------------------------------------------------------- traffic
def _worker(rate, seconds, seed, results):
    """One producer process: `rate` events/s of realistic sessions."""
    random.seed(seed)
    producer = make_producer()
    errors = [0]

    def on_error(_exc):
        errors[0] += 1

    sent = 0
    start = time.time()
    end = start + seconds
    while True:
        now = time.time()
        if now >= end:
            break
        due = int((now - start) * rate)
        while sent < due:
            user = random.randrange(1, LOAD_USERS)
            for event in make_session(user):
                producer.send(config.TOPIC_EVENTS, key=user,
                              value=event).add_errback(on_error)
                sent += 1
        time.sleep(0.005)
    producer.flush()
    elapsed = time.time() - start
    producer.close()
    results.put((sent, errors[0], elapsed))


def run_traffic(rate, seconds, workers):
    """Send `rate` events/s split over `workers` processes; return totals."""
    # spawn, not fork: the parent already runs a KafkaProducer with its own
    # I/O thread, and forking a process mid-I/O can copy held locks.
    ctx = mp.get_context("spawn")
    results = ctx.Queue()
    procs = [ctx.Process(target=_worker,
                         args=(rate / workers, seconds, random.random(), results))
             for _ in range(workers)]
    for p in procs:
        p.start()
    totals = [results.get() for _ in procs]
    for p in procs:
        p.join()
    sent = sum(t[0] for t in totals)
    errors = sum(t[1] for t in totals)
    elapsed = max(t[2] for t in totals)
    return sent, errors, sent / elapsed


class Probes(threading.Thread):
    """Sends latency probes while a step runs."""

    def __init__(self, producer, state, trend_every=5.0, pair_every=20.0):
        super().__init__(daemon=True)
        self.producer = producer
        self.state = state                  # shared across steps
        self.trend_every = trend_every
        self.pair_every = pair_every
        self.stop_flag = threading.Event()
        self.trend_sent = {}                # product_id -> send time
        self.pair_sent = {}                 # lower product_id -> send time

    def _send(self, product, session, ts):
        self.producer.send(config.TOPIC_EVENTS, key=PROBE_USER, value={
            "event_id": uuid.uuid4().hex, "session_id": session,
            "user_id": PROBE_USER, "product_id": product,
            "event_type": "view", "timestamp": ts})

    def run(self):
        next_trend = next_pair = time.time()
        while not self.stop_flag.is_set():
            now = time.time()
            if now >= next_trend:
                product = TREND_PROBE_BASE + self.state["trend"]
                self.state["trend"] += 1
                self._send(product, uuid.uuid4().hex, now)
                self.producer.flush()
                self.trend_sent[product] = time.time()
                next_trend += self.trend_every
            if now >= next_pair:
                first = PAIR_PROBE_BASE + 2 * self.state["pair"]
                self.state["pair"] += 1
                session = uuid.uuid4().hex
                self._send(first, session, now)
                self._send(first + 1, session, now + 0.5)
                self.producer.flush()
                self.pair_sent[first] = time.time()
                next_pair += self.pair_every
            self.stop_flag.wait(0.2)

    def stop(self):
        self.stop_flag.set()
        self.join(timeout=10)


# -------------------------------------------------------------- measuring
def db():
    return mongo.client(config.MONGO_URI)[config.MONGO_DB]


def first_writes(collection, id_field, ids):
    """{id: epoch of its first write} for the ids already in MongoDB."""
    if not ids:
        return {}
    found = {}
    for doc in db()[collection].find({id_field: {"$in": list(ids)}},
                                     {id_field: 1, "_updated_at": 1}):
        at = bench.to_epoch(doc.get("_updated_at"))
        key = doc[id_field]
        if at is not None and (key not in found or at < found[key]):
            found[key] = at
    return found


def latencies(collection, id_field, sent, timeout):
    """Wait up to `timeout` for every probe; return (latencies, lost count)."""
    if not sent:
        return [], 0
    found = bench.wait_until(
        lambda: (lambda f: f if len(f) == len(sent) else None)(
            first_writes(collection, id_field, sent)),
        timeout=timeout, interval=2) or first_writes(collection, id_field, sent)
    values = [max(found[k] - sent[k], 0.0) for k in sent if k in found]
    return values, len(sent) - len(values)


def progress_docs(since, until):
    """The Spark job's own per-batch records between two epoch times."""
    from datetime import datetime, timezone
    lo = datetime.fromtimestamp(since, timezone.utc)
    hi = datetime.fromtimestamp(until, timezone.utc)
    return list(db()["pipeline_metrics"].find(
        {"recorded_at": {"$gte": lo, "$lte": hi}}).sort("recorded_at", 1))


def lag_of(doc):
    values = [v for v in (doc.get("kafka_lag") or []) if v is not None]
    return max(values) if values else None


def wait_for_drain(after, timeout):
    """Seconds from `after` until every query reports a small backlog."""
    drained = {}

    def check():
        for doc in progress_docs(after, time.time()):
            q = doc.get("query")
            lag = lag_of(doc)
            if q in QUERIES and q not in drained and lag is not None \
                    and lag <= DRAINED_BELOW:
                drained[q] = bench.to_epoch(doc["recorded_at"])
        return len(drained) == len(QUERIES)

    if bench.wait_until(check, timeout=timeout, interval=3):
        return round(max(drained.values()) - after, 1)
    return None


def step_metrics(since, until, trigger):
    docs = progress_docs(since + trigger, until)
    by_query = {q: [d for d in docs if d.get("query") == q] for q in QUERIES}
    trend = by_query["trending"]
    pairs = by_query["product_pairs"]
    # The backlog climbs from the idle level to its steady level during the
    # first batches of a step; judging "rising" on those would call every
    # step rising. The first 1 000/s run was marked "not kept up" that way.
    settled_from = since + WARMUP_TRIGGERS * trigger
    lag_series = [lag_of(d) for d in trend + pairs]
    durations = [d.get("batch_duration_ms") for d in trend + pairs]

    def avg(values):
        values = [v for v in values if v is not None]
        return round(sum(values) / len(values), 1) if values else None

    states = [sum(v for v in (d.get("state_rows") or []) if v is not None)
              for d in pairs]
    return {
        "spark_read_rate": avg(d.get("input_rows_per_second") for d in trend),
        "spark_processed_rate": avg(d.get("processed_rows_per_second")
                                    for d in trend),
        "batch_p50_ms": bench.percentile(durations, 50),
        "batch_max_ms": max([v for v in durations if v is not None], default=None),
        "lag_max": max([v for v in lag_series if v is not None], default=None),
        "lag_samples": [lag_of(d) for d in trend
                        if bench.to_epoch(d["recorded_at"]) >= settled_from],
        "pair_state_rows_max": max(states, default=None),
        "batches": len(trend),
    }


def cleanup():
    database = db()
    trend = database[config.COLL_TRENDING].delete_many(
        {"product_id": {"$gte": PROBE_FLOOR, "$lt": 1_000_000}})
    pairs = database[config.COLL_PAIRS].delete_many(
        {"$or": [{"product_id": {"$gte": PROBE_FLOOR, "$lt": 1_000_000}},
                 {"related_product_id": {"$gte": PROBE_FLOOR, "$lt": 1_000_000}}]})
    return trend.deleted_count + pairs.deleted_count


def heartbeat(producer, stop):
    """One event every 2 s, so event time keeps moving during the final wait."""
    while not stop.is_set():
        producer.send(config.TOPIC_EVENTS, key=PROBE_USER, value={
            "event_id": uuid.uuid4().hex, "session_id": uuid.uuid4().hex,
            "user_id": PROBE_USER, "product_id": HEARTBEAT_PRODUCT,
            "event_type": "view", "timestamp": time.time()})
        stop.wait(2)


# ----------------------------------------------------------------- report
def fmt(value, digits=0, unit=""):
    if value is None:
        return "-"
    return f"{value:,.{digits}f}{unit}"


def hyphenate(duration):
    """'2 minutes' -> '2-minute', as in 'a 2-minute watermark'."""
    number, _, unit = (duration or "").strip().partition(" ")
    return f"{number}-{unit.rstrip('s')}" if unit else duration


def write_report(rows, pair_values, pair_lost, env, partitions, args, trigger):
    sustained = [r["target_rate"] for r in rows if r["keeping_up"]]
    best = max(sustained, default=None)
    window = bench.seconds_in(config.COOCCURRENCE_WINDOW)
    floor = window + bench.seconds_in(config.CO_VIEW_GAP) \
        + bench.seconds_in(config.WATERMARK)

    out = ["## Throughput and latency", ""]
    out.append(f"Measured {env['measured_at']} with "
               f"`docker compose run --rm loadtest`, {args.seconds} s per rate, "
               f"{args.workers} producer processes.")
    out.append("")
    out += [
        "| Machine | |", "|---|---|",
        f"| CPU | {env['cpu']} |",
        f"| Cores / memory visible to Docker | {env['cores']} / "
        f"{fmt(env['memory_gb'], 1)} GB |",
        f"| Kafka partitions | {partitions or '-'} |",
        f"| Spark | `local[8]`, {config.SHUFFLE_PARTITIONS} shuffle partitions, "
        f"{config.STATE_STORE} state store, trigger {config.TRIGGER_INTERVAL} |",
        "",
    ]
    if best:
        out.append(f"**Highest rate that kept up: {best:,} events/s.** "
                   "Kept up means Spark read at least 90% of what was sent, "
                   "the unread backlog did not climb once the step had "
                   "settled, and Spark cleared what was left within two "
                   "trigger intervals.")
    else:
        out.append("**No tested rate kept up** - see the table.")
    out.append("")
    out += [
        "| Target events/s | Sent events/s | Spark read events/s | "
        "Batch p50 / max | Max unread | Cleared after | "
        "Event -> trending row p50 / p95 | Join state rows | Kept up |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|:--|",
    ]
    for r in rows:
        kept = {True: "yes", False: "**no**", None: "unclear"}[r["keeping_up"]]
        out.append(
            f"| {r['target_rate']:,} | {fmt(r['achieved_rate'])} | "
            f"{fmt(r['spark_read_rate'])} | "
            f"{fmt(r['batch_p50_ms'] and r['batch_p50_ms'] / 1000, 1)} / "
            f"{fmt(r['batch_max_ms'] and r['batch_max_ms'] / 1000, 1)} s | "
            f"{fmt(r['lag_max'])} | {fmt(r['drain_s'], 0, ' s')} | "
            f"{fmt(r['trend_p50_s'], 1)} / {fmt(r['trend_p95_s'], 1)} s | "
            f"{fmt(r['pair_state_rows_max'])} | {kept} |")
    out.append("")
    out.append(bench.xychart(
        "Events per second: sent (bars) and read by Spark (line)",
        [f"{r['target_rate'] / 1000:g}k" if r["target_rate"] >= 1000
         else str(r["target_rate"]) for r in rows],
        "events/s",
        [r["achieved_rate"] for r in rows],
        [r["spark_read_rate"] for r in rows]))
    out.append("")
    out += [
        "How to read it:",
        "",
        "- **Sent** is what the load generator achieved; it shares the CPU with "
        "Spark, so at high targets it can fall short of the target itself.",
        "- **Spark read** is the trending query's own input rate. The pairing "
        "query reads the topic twice (it joins the stream with itself), so its "
        "figure is double.",
        "- **Max unread** is the largest Kafka backlog Spark reported after a "
        "batch. Some backlog is normal: events keep arriving while a batch "
        "runs.",
        "- **Join state rows** is the recent events the co-occurrence join "
        "holds to find pairs. It grows with the rate but stays bounded by the "
        f"{hyphenate(config.CO_VIEW_GAP)} co-view gap, and RocksDB keeps it "
        "off the JVM heap (ADR 0001, ADR 0009).",
        "- **Event -> trending row** is measured with probe events: the time "
        "from sending one event to its trending row being written. It "
        "includes up to one trigger interval of waiting "
        f"({config.TRIGGER_INTERVAL}) plus the batch itself.",
        "",
        "### Event -> product pair",
        "",
    ]
    if pair_values:
        out.append(
            f"{len(pair_values)} pair probes: p50 "
            f"{fmt(bench.percentile(pair_values, 50) / 60, 1)} min, "
            f"p95 {fmt(bench.percentile(pair_values, 95) / 60, 1)} min, "
            f"max {fmt(max(pair_values) / 60, 1)} min"
            + (f" ({pair_lost} not written within the wait)." if pair_lost
               else "."))
    else:
        out.append("No pair probe was written within the wait.")
    out += [
        "",
        f"This delay is by design, not load: a pair is written once its "
        f"{hyphenate(config.COOCCURRENCE_WINDOW)} window has closed, the "
        f"{hyphenate(config.CO_VIEW_GAP)} co-view gap has passed and the "
        f"{hyphenate(config.WATERMARK)} watermark has moved beyond both - about "
        f"{floor / 60:.0f} minutes plus up to one trigger. See ADR 0001 and "
        "ADR 0002.",
    ]
    bench.update_section("throughput", "\n".join(out))


def write_csv(rows):
    os.makedirs("results", exist_ok=True)
    path = os.path.join("results", "load_test.csv")
    fields = [k for k in rows[0] if k != "lag_samples"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


# ------------------------------------------------------------------- main
def main():
    sys.stdout.reconfigure(line_buffering=True)
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--rates", type=int, nargs="+", default=DEFAULT_RATES,
                        help="target events/s, one step each")
    parser.add_argument("--seconds", type=int, default=90,
                        help="seconds of load per step (default 90)")
    parser.add_argument("--workers", type=int, default=6,
                        help="producer processes (default 6)")
    parser.add_argument("--drain-timeout", type=int, default=300,
                        help="max seconds to wait for Spark to catch up")
    parser.add_argument("--pair-wait", type=int, default=480,
                        help="max seconds to wait for pair probes at the end")
    args = parser.parse_args()

    try:
        health = requests.get(f"{config.API_BASE_URL}/health", timeout=10).json()
    except requests.RequestException:
        health = None
    if not health or health.get("status") != "ok":
        print("API not healthy. Start the stack first: docker compose up -d")
        return 1

    trigger = bench.seconds_in(config.TRIGGER_INTERVAL) or 10.0
    env = bench.environment()
    latest = kafka_io.latest_offsets(config.TOPIC_EVENTS, config.KAFKA_BOOTSTRAP)
    partitions = len(latest) if latest else None
    print(f"Kafka {config.KAFKA_BOOTSTRAP} ({partitions} partitions) | "
          f"MongoDB {config.MONGO_URI} | {env['cores']} cores")
    print(f"Steps: {args.rates} events/s, {args.seconds}s each, "
          f"{args.workers} producer processes\n")

    removed = cleanup()
    if removed:
        print(f"Removed {removed} probe rows left by an earlier run.\n")

    probe_producer = make_producer()
    probe_state = {"trend": 0, "pair": 0}
    all_pairs = {}
    rows = []
    try:
        for rate in args.rates:
            print(f"--- {rate:,} events/s for {args.seconds}s ---")
            probes = Probes(probe_producer, probe_state)
            started = time.time()
            probes.start()
            sent, errors, achieved = run_traffic(rate, args.seconds, args.workers)
            ended = time.time()
            probes.stop()
            all_pairs.update(probes.pair_sent)
            print(f"    sent {sent:,} events ({achieved:,.0f}/s), "
                  f"{errors} send errors")

            drain = wait_for_drain(ended, args.drain_timeout)
            print("    backlog cleared "
                  + (f"{drain:.0f}s after the load stopped" if drain is not None
                     else f"NOT within {args.drain_timeout}s"))
            metrics = step_metrics(started, ended, trigger)
            trend_values, trend_lost = latencies(
                config.COLL_TRENDING, "product_id", probes.trend_sent,
                timeout=60)
            row = {
                "target_rate": rate,
                "events_sent": sent,
                "achieved_rate": round(achieved, 1),
                "send_errors": errors,
                **metrics,
                "drain_s": drain,
                "trend_probes": len(probes.trend_sent),
                "trend_lost": trend_lost,
                "trend_p50_s": bench.percentile(trend_values, 50),
                "trend_p95_s": bench.percentile(trend_values, 95),
                "trend_max_s": max(trend_values, default=None),
            }
            row["lag_rising"] = bench.rising_floor(metrics["lag_samples"])
            row["keeping_up"] = bench.keeping_up(
                metrics["lag_samples"], drain, trigger,
                read_rate=row["spark_read_rate"], sent_rate=row["achieved_rate"])
            rows.append(row)
            print(f"    Spark read {fmt(row['spark_read_rate'])}/s | batch p50 "
                  f"{fmt(row['batch_p50_ms'])} ms, max {fmt(row['batch_max_ms'])} ms"
                  f" | max unread {fmt(row['lag_max'])}")
            print(f"    event -> trending row: p50 {fmt(row['trend_p50_s'], 1)}s, "
                  f"p95 {fmt(row['trend_p95_s'], 1)}s ({trend_lost} lost)")
            print(f"    kept up: {row['keeping_up']}\n")

        print(f"Waiting up to {args.pair_wait}s for the "
              f"{len(all_pairs)} pair probes (windows must close)...")
        stop = threading.Event()
        beat = threading.Thread(target=heartbeat, args=(probe_producer, stop),
                                daemon=True)
        beat.start()
        pair_values, pair_lost = latencies(
            config.COLL_PAIRS, "product_id", all_pairs, timeout=args.pair_wait)
        stop.set()
        beat.join(timeout=5)
    finally:
        probe_producer.flush()
        probe_producer.close()
        removed = cleanup()

    if not rows:
        return 1
    csv_path = write_csv(rows)
    write_report(rows, pair_values, pair_lost, env, partitions, args, trigger)

    print("=" * 72)
    for r in rows:
        print(f"{r['target_rate']:>7,}/s  sent {fmt(r['achieved_rate']):>7}/s  "
              f"read {fmt(r['spark_read_rate']):>7}/s  "
              f"cleared {fmt(r['drain_s'], 0, 's'):>5}  "
              f"latency p95 {fmt(r['trend_p95_s'], 1, 's'):>6}  "
              f"kept up: {r['keeping_up']}")
    if pair_values:
        print(f"\nevent -> pair: p50 {bench.percentile(pair_values, 50) / 60:.1f} "
              f"min over {len(pair_values)} probes ({pair_lost} not written)")
    print(f"\nRemoved {removed} probe rows. Wrote {csv_path} and "
          f"{bench.REPORT_PATH}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
