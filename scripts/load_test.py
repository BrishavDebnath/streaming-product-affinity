"""
Load test: ramp the event rate and record where the pipeline stops keeping up.

Produces the one number a data-engineering interviewer actually asks for —
"what throughput does it sustain, and how do you know?" — instead of the rate
you happened to configure.

    python scripts/load_test.py                       # default ramp
    python scripts/load_test.py --rates 200 1000 5000 --seconds 90

For each rate it produces events for `--seconds`, then polls /pipeline until
lag stabilises, and records processing lag and achieved send rate. Results go
to docs/BENCHMARKS.md and results/load_test.csv.

Read the output like this: the highest rate where lag stays flat is your
sustainable throughput. Once lag climbs batch over batch and never recovers,
the pipeline is falling behind and the queue is growing without bound.
"""

import argparse
import csv
import os
import statistics
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from kafka import KafkaProducer

from src.common import catalog, config
from src.common.kafka_io import JsonValueSerializer, StringKeySerializer

DEFAULT_RATES = [100, 500, 1000, 2500, 5000]


def api(path):
    try:
        r = requests.get(f"{config.API_BASE_URL}{path}", timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException:
        return None


def build_producer():
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        value_serializer=JsonValueSerializer(),
        key_serializer=StringKeySerializer(),
        acks=1,            # throughput test, not a durability test
        linger_ms=50,
        batch_size=64 * 1024,
    )


def blast(producer, target_rate, seconds):
    """Send at `target_rate` for `seconds`; return the rate actually achieved."""
    products = catalog.product_ids()
    users = catalog.USERS
    interval = 1.0 / target_rate
    sent = 0
    started = time.time()
    next_send = started

    while time.time() - started < seconds:
        now = time.time()
        if now < next_send:
            # Busy-wait below ~2 ms; time.sleep is too coarse at high rates.
            if next_send - now > 0.002:
                time.sleep(next_send - now)
            continue
        session = str(uuid.uuid4())
        user = users[sent % len(users)]
        for offset in range(2):                       # a 2-event session
            producer.send(config.TOPIC_EVENTS, key=user, value={
                "event_id": f"load-{sent}-{offset}",
                "session_id": session,
                "user_id": user,
                "product_id": products[(sent + offset) % len(products)],
                "event_type": "view",
                "timestamp": time.time(),
            })
            sent += 1
        next_send += interval * 2

    producer.flush()
    elapsed = time.time() - started
    return sent, sent / elapsed


def settle(samples, timeout):
    """Poll /pipeline, returning the lag readings observed."""
    lags = []
    deadline = time.time() + timeout
    while time.time() < deadline and len(lags) < samples:
        time.sleep(10)
        pipe = api("/pipeline")
        if pipe and pipe.get("lag_seconds") is not None:
            lags.append(pipe["lag_seconds"])
            print(f"    lag {pipe['lag_seconds']:>7.1f}s   "
                  f"age {pipe['staleness_seconds']:>6.0f}s")
    return lags


def main():
    sys.stdout.reconfigure(line_buffering=True)   # show progress live
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rates", type=int, nargs="+", default=DEFAULT_RATES)
    parser.add_argument("--seconds", type=int, default=60,
                        help="seconds of load per rate")
    parser.add_argument("--settle", type=int, default=120,
                        help="max seconds to watch lag after each burst")
    args = parser.parse_args()

    health = api("/health")
    if not health or health.get("status") != "ok":
        print("API not healthy. Start the stack first: docker compose up -d --build")
        return 1

    print(f"Kafka {config.KAFKA_BOOTSTRAP} | API {config.API_BASE_URL}")
    print(f"Ramp: {args.rates} events/s, {args.seconds}s each\n")

    producer = build_producer()
    results = []
    try:
        for rate in args.rates:
            print(f"--- target {rate} events/s ---")
            sent, achieved = blast(producer, rate, args.seconds)
            print(f"    sent {sent:,} in {args.seconds}s "
                  f"-> achieved {achieved:,.0f} events/s")
            lags = settle(5, args.settle)
            if lags:
                row = {
                    "target_rate": rate,
                    "achieved_rate": round(achieved, 1),
                    "events_sent": sent,
                    "lag_min": round(min(lags), 2),
                    "lag_median": round(statistics.median(lags), 2),
                    "lag_max": round(max(lags), 2),
                    "lag_rising": lags[-1] > lags[0] * 1.5,
                }
            else:
                row = {"target_rate": rate, "achieved_rate": round(achieved, 1),
                       "events_sent": sent, "lag_min": None, "lag_median": None,
                       "lag_max": None, "lag_rising": None}
            results.append(row)
            print()
    finally:
        producer.close()

    os.makedirs("results", exist_ok=True)
    csv_path = os.path.join("results", "load_test.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)

    sustainable = [r for r in results if r["lag_rising"] is False]
    ceiling = max((r["target_rate"] for r in sustainable), default=None)

    os.makedirs("docs", exist_ok=True)
    with open(os.path.join("docs", "BENCHMARKS.md"), "w", encoding="utf-8") as f:
        f.write("# Benchmarks\n\nMeasured with `python scripts/load_test.py`, "
                f"{args.seconds}s of load per rate.\n\n")
        f.write("Fill in your hardware: CPU, RAM, Docker memory limit.\n\n")
        f.write("| target events/s | achieved | lag median (s) | lag max (s) | keeping up |\n")
        f.write("|---:|---:|---:|---:|:--|\n")
        for r in results:
            keeping = ("yes" if r["lag_rising"] is False
                       else "no" if r["lag_rising"] else "unknown")
            f.write(f"| {r['target_rate']:,} | {r['achieved_rate']:,.0f} | "
                    f"{r['lag_median']} | {r['lag_max']} | {keeping} |\n")
        if ceiling:
            f.write(f"\n**Sustained throughput: ~{ceiling:,} events/s** — the "
                    "highest rate at which processing lag stayed flat rather "
                    "than growing batch over batch.\n")

    print("=" * 60)
    for r in results:
        state = ("keeping up" if r["lag_rising"] is False
                 else "FALLING BEHIND" if r["lag_rising"] else "no data")
        print(f"{r['target_rate']:>6,} /s -> achieved {r['achieved_rate']:>7,.0f} "
              f"| lag median {r['lag_median']} | {state}")
    if ceiling:
        print(f"\nSustained throughput: ~{ceiling:,} events/s")
    print(f"\nWrote {csv_path} and docs/BENCHMARKS.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
