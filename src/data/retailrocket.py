"""
Turning the RetailRocket export into events this pipeline can consume.

The dataset is a flat log: ``timestamp,visitorid,event,itemid,transactionid``,
2.7M rows from four and a half months of a real shop in 2015. Three things
have to happen before it can be replayed:

1. **Sessions.** RetailRocket has no session id, only a visitor id. A visitor
   who came back three weeks later is not still in the same visit, and pairing
   those two products would be nonsense - so visits are cut at
   ``SESSION_GAP_MINUTES`` of inactivity, the industry-standard rule.

2. **Event names.** ``view``/``addtocart``/``transaction`` become this
   project's ``view``/``add_to_cart``/``purchase``, so the existing weights
   apply.

3. **Time.** The events are from 2015. Replayed as-is, every window the
   pipeline wrote would sit eleven years in the past, the API's "last 30
   minutes" lookback would return nothing, and the dashboard would look
   broken. Timestamps are therefore mapped onto the replay's own clock,
   preserving the ORDER and the RELATIVE spacing of events, compressed by a
   fixed factor. See docs/adr/0011.

Everything here is pure: no Kafka, no Mongo, no files opened for you. The
scripts do the I/O, these functions do the thinking, and the tests can check
them without a dataset present.
"""

from __future__ import annotations

import csv
import hashlib
from collections.abc import Iterable, Iterator
from typing import NamedTuple

# RetailRocket's own column names, checked on load so a changed export fails
# loudly instead of silently producing zero events.
EVENT_COLUMNS = ["timestamp", "visitorid", "event", "itemid", "transactionid"]
PROPERTY_COLUMNS = ["timestamp", "itemid", "property", "value"]

# A visit ends after this much inactivity from the same visitor.
SESSION_GAP_MINUTES = 30

# RetailRocket event name -> this project's event type. The pipeline weights
# these (view 1.0, add_to_cart 3.0, purchase 5.0), so the mapping decides how
# much each row counts towards affinity.
EVENT_TYPES = {
    "view": "view",
    "addtocart": "add_to_cart",
    "transaction": "purchase",
}


class Event(NamedTuple):
    """One row of the dataset, parsed. ``at`` is epoch seconds."""

    at: float
    visitor: int
    item: int
    event_type: str


def parse_rows(rows: Iterable[dict]) -> Iterator[Event]:
    """Parse dataset rows, skipping ones this pipeline cannot use.

    Unparseable numbers and unknown event names are dropped rather than
    raising: a 2.7M-row public dataset with three bad lines should not stop a
    replay. The counts are reported by the caller, so a mapping that drops
    everything is visible instead of silent.
    """
    for row in rows:
        event_type = EVENT_TYPES.get((row.get("event") or "").strip())
        if event_type is None:
            continue
        try:
            at = int(row["timestamp"]) / 1000.0          # dataset is in ms
            visitor = int(row["visitorid"])
            item = int(row["itemid"])
        except (KeyError, TypeError, ValueError):
            continue
        yield Event(at, visitor, item, event_type)


def read_events(path: str) -> Iterator[Event]:
    """Stream events.csv. 2.7M rows never all sit in memory."""
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = set(EVENT_COLUMNS) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"{path} is missing columns {sorted(missing)}; expected the "
                f"RetailRocket events.csv with {EVENT_COLUMNS}")
        yield from parse_rows(reader)


def session_id(visitor: int, started_at: float) -> str:
    """A stable id for one visit.

    Derived from the visitor and when the visit began, so re-running the
    sessioniser on the same data gives the same ids - which is what lets a
    replay be resumed, or an evaluation compare two runs.
    """
    digest = hashlib.sha1(f"{visitor}:{started_at:.0f}".encode()).hexdigest()
    return f"rr-{digest[:16]}"


class Visit(NamedTuple):
    session: str
    visitor: int
    events: list[Event]

    @property
    def started_at(self) -> float:
        return self.events[0].at

    @property
    def items(self) -> list[int]:
        """Distinct items in view order - what a recommender is judged on."""
        seen: dict[int, None] = {}
        for event in self.events:
            seen.setdefault(event.item, None)
        return list(seen)


def sessionise(events: Iterable[Event],
               gap_minutes: int = SESSION_GAP_MINUTES) -> Iterator[Visit]:
    """Group a visitor's events into visits, cutting at an inactivity gap.

    Events must arrive sorted by visitor, then time - `sort_events` does that.
    Working in that order keeps memory flat: one visit is held at a time,
    never the whole dataset.
    """
    gap = gap_minutes * 60
    current: list[Event] = []

    def finish(events: list[Event]) -> Visit:
        return Visit(session_id(events[0].visitor, events[0].at),
                     events[0].visitor, list(events))

    for event in events:
        if current and (event.visitor != current[0].visitor
                        or event.at - current[-1].at > gap):
            yield finish(current)
            current = []
        current.append(event)
    if current:
        yield finish(current)


def sort_events(events: Iterable[Event]) -> list[Event]:
    """Order for `sessionise`: by visitor, then time."""
    return sorted(events, key=lambda e: (e.visitor, e.at))


class Clock:
    """Maps dataset time onto replay time.

    ``speedup`` is how many seconds of 2015 pass per second of replay. The
    first event lands at ``starts_at``; everything after keeps its relative
    spacing, divided by the speedup. Order is never changed, so a session's
    events stay inside the pipeline's co-view gap as long as
    ``session length / speedup`` is shorter than the gap.
    """

    def __init__(self, first_event_at: float, starts_at: float,
                 speedup: float):
        if speedup <= 0:
            raise ValueError("speedup must be positive")
        self.first_event_at = first_event_at
        self.starts_at = starts_at
        self.speedup = speedup

    def at(self, dataset_time: float) -> float:
        return self.starts_at + (dataset_time - self.first_event_at) / self.speedup

    def span(self, dataset_seconds: float) -> float:
        return dataset_seconds / self.speedup


def to_wire(event: Event, session: str, clock: Clock, sequence: int) -> dict:
    """One dataset event in the pipeline's v2 wire format.

    `event_id` is derived from the row rather than random, so replaying the
    same slice twice produces the same ids and the idempotent Mongo writes
    genuinely de-duplicate instead of double-counting.
    """
    return {
        "event_id": f"{session}-{sequence}",
        "session_id": session,
        "user_id": event.visitor,
        "product_id": event.item,
        "timestamp": clock.at(event.at),
        "schema_version": 2,
        "action": event.event_type,
        "channel": "replay",
    }


def build_catalog(items: Iterable[int],
                  categories: dict[int, int] | None = None) -> list[dict]:
    """A catalogue for real items.

    RetailRocket's item properties are hashed: there are no names or prices to
    show, only a category id. So a product is labelled by its id and grouped
    by its category, which is all the API and the dashboard need - and it is
    honest about what the data contains rather than inventing names.
    """
    categories = categories or {}
    catalog = []
    for item in sorted(set(items)):
        category = categories.get(item)
        catalog.append({
            "id": item,
            "name": f"Item {item}",
            "price": None,
            "category": f"cat-{category}" if category is not None else "uncategorised",
        })
    return catalog


def read_categories(path: str, wanted: set[int] | None = None) -> dict[int, int]:
    """item id -> category id, from item_properties_part*.csv.

    The file records every property change over time, so an item can appear
    many times; the LAST categoryid row wins, which is the category the item
    ended the period in.
    """
    latest: dict[int, tuple[float, int]] = {}
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = set(PROPERTY_COLUMNS) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"{path} is missing columns {sorted(missing)}; expected "
                f"{PROPERTY_COLUMNS}")
        for row in reader:
            if row.get("property") != "categoryid":
                continue
            try:
                item = int(row["itemid"])
                if wanted is not None and item not in wanted:
                    continue
                at = float(row["timestamp"])
                category = int(row["value"])
            except (KeyError, TypeError, ValueError):
                continue
            seen = latest.get(item)
            if seen is None or at > seen[0]:
                latest[item] = (at, category)
    return {item: category for item, (_, category) in latest.items()}
