#!/usr/bin/env python3
"""
Replay real RetailRocket traffic through Kafka.

    python scripts/replay.py --days 7                 # a week, in ~5 minutes
    python scripts/replay.py --days 7 --minutes 10    # slower, more realistic
    python scripts/replay.py --days 1 --dry-run       # parse only, send nothing

What it does, in order: read the slice, cut it into visits at a 30-minute
inactivity gap, map the timestamps onto the replay's own clock (2015 events
would fall outside every lookback the API has), and send them to Kafka in
event-time order at the pace that clock implies.

It also writes a catalogue of the items in the slice, so the API and the
dashboard can label real products:

    CATALOG_FILE=data/catalog_retailrocket.json docker compose up -d

Sending the same slice twice is safe: event ids are derived from the data, so
the pipeline's upserts overwrite rather than double-count.
"""

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kafka import KafkaProducer  # noqa: E402

from src.common import config  # noqa: E402
from src.common.kafka_io import JsonValueSerializer, StringKeySerializer  # noqa: E402
from src.data import retailrocket as rr  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
CATALOG_OUT = ROOT / "data" / "catalog_retailrocket.json"

# Visits longer than this, once compressed, would have their first and last
# event further apart than the pipeline's co-view gap, so their products would
# never pair. Checked against the chosen speedup and reported.
CO_VIEW_GAP_SECONDS = 120

# Spark's watermark only moves when newer events arrive, so the last windows
# of a replay would sit unemitted forever once the replay stops - up to 40% of
# a five-minute slice. These events carry timestamps past the end of the slice
# and push the watermark over the line. Each is its own session and uses one
# reserved product id, so they can form no pairs, and their trending rows are
# deleted again below.
FLUSH_PRODUCT = -1
FLUSH_MINUTES = 10

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s replay | %(message)s")
log = logging.getLogger("replay")

_RUNNING = True


def _stop(signum, _frame):
    global _RUNNING
    _RUNNING = False
    log.info("signal %s received, stopping after the current batch", signum)


def parse_day(value: str) -> float:
    return datetime.strptime(value, "%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp()


def day_of(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%d")


def load_slice(path: Path, start: float | None, days: float, limit: int):
    """Events inside the window, and where the dataset itself starts."""
    events = []
    dataset_start = None
    end = None
    for event in rr.read_events(str(path)):
        if dataset_start is None or event.at < dataset_start:
            dataset_start = event.at
        if start is None:
            start = dataset_start
            end = start + days * 86400
        if event.at < start:
            continue
        if end is not None and event.at >= end:
            continue
        events.append(event)
        if limit and len(events) >= limit:
            break
    return events, dataset_start


def visits_of(events):
    return list(rr.sessionise(rr.sort_events(events)))


def wire_events(visits, clock):
    """Every event in send order: by replay time, with its session attached.

    Each row also carries the ORIGINAL dataset time. The wire event's own
    timestamp has already been mapped onto the replay clock, so reading the
    day back out of it would only ever report today.
    """
    out = []
    for visit in visits:
        for sequence, event in enumerate(visit.events):
            out.append((clock.at(event.at),
                        rr.to_wire(event, visit.session, clock, sequence),
                        event.visitor, event.at))
    out.sort(key=lambda row: row[0])
    return out


def write_catalog(items, dest: Path) -> int:
    """A catalogue for the replayed items, with real category ids if present."""
    categories: dict[int, int] = {}
    for part in ("item_properties_part1.csv", "item_properties_part2.csv"):
        path = RAW / part
        if path.is_file():
            categories.update(rr.read_categories(str(path), wanted=set(items)))
    products = rr.build_catalog(items, categories)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(products), encoding="utf-8")
    known = sum(1 for p in products if p["category"] != "uncategorised")
    log.info("wrote %s (%s items, %s with a category)", dest, len(products), known)
    return len(products)


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        value_serializer=JsonValueSerializer(),
        key_serializer=StringKeySerializer(),
        acks=1,                    # replay speed matters more than durability
        retries=5,
        linger_ms=20,
    )


def flush_windows(producer, after: float) -> int:
    """Advance the watermark past the end of the replay.

    Without this the windows holding the last minutes of the replay never
    close: Structured Streaming moves a watermark on event time, and the
    replay was the only source of it.
    """
    for minute in range(1, FLUSH_MINUTES + 1):
        stamp = after + minute * 60
        producer.send(config.TOPIC_EVENTS, key=FLUSH_PRODUCT, value={
            "event_id": f"flush-{int(stamp)}",
            "session_id": f"flush-{int(stamp)}",     # alone, so it pairs with nothing
            "user_id": FLUSH_PRODUCT,
            "product_id": FLUSH_PRODUCT,
            "timestamp": stamp,
            "schema_version": 2,
            "action": "view",
            "channel": "replay-flush"})
    producer.flush()
    log.info("sent %s watermark events (product %s), reaching %s minutes past "
             "the replay", FLUSH_MINUTES, FLUSH_PRODUCT, FLUSH_MINUTES)
    return FLUSH_MINUTES


def clear_flush_rows(timeout: float = 240.0) -> int:
    """Delete the watermark events' own trending rows once Spark has written
    them, so they never appear in the dashboard or in /trending."""
    from src.common import mongo

    collection = mongo.client(config.MONGO_URI)[config.MONGO_DB][config.COLL_TRENDING]
    removed = 0
    started = last_seen = time.time()
    while time.time() - started < timeout:
        deleted = collection.delete_many({"product_id": FLUSH_PRODUCT}).deleted_count
        if deleted:
            removed += deleted
            last_seen = time.time()
        elif removed and time.time() - last_seen > 60:
            break                       # nothing new for a minute: Spark is done
        time.sleep(10)
    log.info("removed %s watermark rows from %s", removed, config.COLL_TRENDING)
    return removed


def send(rows, dry_run: bool, flush: bool = True) -> int:
    if dry_run:
        log.info("dry run: %s events parsed, nothing sent", len(rows))
        return 0
    producer = build_producer()
    log.info("connected to %s, topic=%s", config.KAFKA_BOOTSTRAP,
             config.TOPIC_EVENTS)

    started = time.time()
    sent = 0
    last_report = started
    try:
        for at, event, visitor, dataset_time in rows:
            if not _RUNNING:
                break
            behind = at - time.time()
            if behind > 0:
                time.sleep(min(behind, 1.0))
            producer.send(config.TOPIC_EVENTS, key=visitor, value=event)
            sent += 1
            if time.time() - last_report >= 10:
                elapsed = time.time() - started
                log.info("sent=%s/%s (%.0f%%) rate=%.0f/s dataset day=%s",
                         sent, len(rows), 100 * sent / len(rows),
                         sent / max(elapsed, 1e-6),
                         day_of(dataset_time))
                last_report = time.time()
        producer.flush()
        if flush and sent:
            # The replay was the only thing moving the watermark; without
            # this the windows holding its last minutes never close.
            flush_windows(producer, rows[-1][0])
    finally:
        producer.close()
    elapsed = time.time() - started
    log.info("sent %s events in %.0fs (%.0f/s)", sent, elapsed,
             sent / max(elapsed, 1e-6))
    return sent


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, default=RAW / "events.csv")
    parser.add_argument("--days", type=float, default=7.0,
                        help="how many days of the dataset to replay")
    parser.add_argument("--start", type=str, default=None,
                        help="first dataset day to replay, YYYY-MM-DD "
                             "(default: where the dataset begins)")
    parser.add_argument("--minutes", type=float, default=5.0,
                        help="how long the replay should take")
    parser.add_argument("--limit", type=int, default=0,
                        help="stop after N events (0 = the whole slice)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-catalog", action="store_true",
                        help="skip writing data/catalog_retailrocket.json")
    parser.add_argument("--no-flush", action="store_true",
                        help="do not send the watermark events that close the "
                             "replay's last windows")
    parser.add_argument("--flush-only", action="store_true",
                        help="send only those watermark events, to close the "
                             "windows of a replay that has already run")
    parser.add_argument("--keep-flush-rows", action="store_true",
                        help="leave the watermark events' own trending rows "
                             "in MongoDB instead of deleting them")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    if args.flush_only:
        producer = build_producer()
        try:
            flush_windows(producer, time.time())
        finally:
            producer.close()
        if not args.keep_flush_rows:
            clear_flush_rows()
        log.info("windows close within a trigger interval or two; give the "
                 "pipeline about a minute before evaluating.")
        return 0

    if not args.events.is_file():
        log.error("%s not found - run scripts/fetch_dataset.py first",
                  args.events)
        return 2

    log.info("reading %s", args.events)
    start = parse_day(args.start) if args.start else None
    events, dataset_start = load_slice(args.events, start, args.days, args.limit)
    if not events:
        log.error("the slice is empty; the dataset starts on %s",
                  day_of(dataset_start) if dataset_start else "an unknown day")
        return 1

    visits = visits_of(events)
    span = max(e.at for e in events) - min(e.at for e in events)
    speedup = max(span / (args.minutes * 60), 1.0)
    clock = rr.Clock(min(e.at for e in events), time.time() + 5, speedup)

    longest = max((v.events[-1].at - v.events[0].at) for v in visits)
    log.info("slice: %s events, %s visits, %s to %s (%.1f days)",
             f"{len(events):,}", f"{len(visits):,}",
             day_of(min(e.at for e in events)), day_of(max(e.at for e in events)),
             span / 86400)
    log.info("replaying at %.0fx: %.1f minutes of wall clock, ~%.0f events/s",
             speedup, span / speedup / 60, len(events) / max(span / speedup, 1e-6))
    compressed = clock.span(longest)
    log.info("longest visit is %.0f minutes, %.1fs once compressed (co-view "
             "gap is %ss, so visits %s pair)",
             longest / 60, compressed, CO_VIEW_GAP_SECONDS,
             "still" if compressed < CO_VIEW_GAP_SECONDS else "no longer")

    if not args.no_catalog:
        write_catalog({e.item for e in events}, CATALOG_OUT)

    rows = wire_events(visits, clock)
    sent = send(rows, args.dry_run, flush=not args.no_flush)
    if sent and not args.no_flush and not args.keep_flush_rows:
        clear_flush_rows()
    if sent and not args.dry_run:
        log.info("done. Pairs appear in the dashboard about 5 minutes after "
                 "the events that made them.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
