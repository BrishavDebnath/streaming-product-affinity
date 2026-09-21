#!/usr/bin/env python3
"""
Measure the pipeline against a bestseller baseline on held-out real traffic.

    python scripts/evaluate.py                    # after a replay
    python scripts/evaluate.py --source mongo     # skip the API layer
    python scripts/evaluate.py --test-days 2 --limit 5000

Method, in one paragraph: the replay fed the pipeline the first `--train-days`
of the dataset. This script takes the days AFTER that - traffic the pipeline
has never seen - cuts it into visits, and for each visit hands the model the
first product and asks for ten recommendations. It scores a hit when the
product the shopper actually viewed next is in that ten. The baseline gets the
same test cases and always answers with the ten most-viewed products of the
training period.

Both numbers come from the same cases, so the comparison is fair, and the
pipeline's answers come out of MongoDB (or the live API), not from a
re-implementation of the join. See docs/adr/0011.
"""

import argparse
import json
import logging
import random
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests  # noqa: E402

from src.common import bench, config  # noqa: E402
from src.data import evaluation as ev  # noqa: E402
from src.data import retailrocket as rr  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
REPORT = ROOT / "docs" / "EVALUATION.md"
RESULTS = ROOT / "results"

REPORT_HEADER = """# Evaluation

Measured by `scripts/evaluate.py` on the RetailRocket dataset: real visits from
days the pipeline never saw, scored against the ten most-viewed products of the
training period. Method and caveats: [ADR 0011](adr/0011-real-data-and-evaluation.md).

"""

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s evaluate | %(message)s")
log = logging.getLogger("evaluate")


def day_of(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%d")


def split(path: Path, train_days: float, test_days: float,
          start: str | None):
    """Training views (for the baseline) and the test period's visits."""
    train_views: Counter = Counter()
    test_events = []
    train_events = 0
    # Day zero is the dataset's earliest event, found by scanning - not the
    # first row, which in this export is seven weeks later. scripts/replay.py
    # anchors the same way, so the training window here is the one replayed.
    first, newest = rr.time_span(str(path))
    begin = (datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
             .timestamp() if start else first)

    for event in rr.read_events(str(path)):
        if event.at < begin:
            continue
        offset_days = (event.at - begin) / 86400
        if offset_days < train_days:
            train_events += 1
            if event.event_type == "view":
                train_views[event.item] += 1
        elif offset_days < train_days + test_days:
            test_events.append(event)
        # No `break` here either: stopping at the first row past the window
        # silently truncated the read on an out-of-order export, and with a
        # 30-day window found no test traffic at all.

    visits = list(rr.sessionise(rr.sort_events(test_events)))
    return train_views, train_events, visits, begin, (first, newest)


class ApiRecommender:
    """What a client actually gets: /related-products, fallback and all."""

    def __init__(self, base_url: str, k: int, score_by: str = "affinity"):
        self.base_url = base_url.rstrip("/")
        self.k = k
        self.score_by = score_by
        self.session = requests.Session()
        self.cache: dict[int, list[int]] = {}
        self.sources: Counter = Counter()

    def __call__(self, product_id: int) -> list[int]:
        if product_id in self.cache:
            return self.cache[product_id]
        url = (f"{self.base_url}/related-products/{product_id}"
               f"?limit={self.k}&score_by={self.score_by}")
        try:
            response = self.session.get(url, timeout=10)
        except requests.RequestException as exc:
            raise SystemExit(f"the API at {self.base_url} is not reachable: {exc}\n"
                             f"start the stack, or use --source mongo") from None
        if response.status_code == 404:          # product never seen: no answer
            self.sources["unknown_product"] += 1
            self.cache[product_id] = []
            return []
        response.raise_for_status()
        body = response.json()
        source = body.get("source", "unknown")
        if source == "co_occurrence" and body.get("filled_from_category"):
            source = "co_occurrence_plus_category"
        self.sources[source] += 1
        items = [row["product_id"] for row in body.get("related_products", [])]
        self.cache[product_id] = items
        return items


class MongoRecommender:
    """The pair table alone - no trending fallback, no cache, no HTTP.

    Mirrors the API's own widening: answer from the recent lookback where
    there is something there, otherwise from everything still retained, and
    count which of the two answered. Without that, this number depended on how
    long after a replay the evaluation happened to run - a score that quietly
    decays with the clock is not a measurement.
    """

    def __init__(self, k: int, lookback_minutes: int, widen: bool = True):
        from src.common import mongo
        self.db = mongo.client(config.MONGO_URI)[config.MONGO_DB]
        self.k = k
        self.lookback = lookback_minutes
        self.widen = widen
        self.cache: dict[int, list[int]] = {}
        self.sources: Counter = Counter()

    def _query(self, product_id: int, cutoff=None) -> list[int]:
        match: dict = {"product_id": product_id}
        if cutoff is not None:
            match["window_start"] = {"$gte": cutoff}
        rows = self.db[config.COLL_PAIRS].aggregate([
            {"$match": match},
            {"$group": {"_id": "$related_product_id",
                        "pair_count": {"$sum": "$pair_count"}}},
            {"$match": {"pair_count": {"$gte": config.MIN_PAIR_COUNT}}},
            {"$sort": {"pair_count": -1, "_id": 1}},
            {"$limit": self.k},
        ])
        return [row["_id"] for row in rows]

    def __call__(self, product_id: int) -> list[int]:
        if product_id in self.cache:
            return self.cache[product_id]
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=self.lookback)
        items = self._query(product_id, cutoff)
        source = "co_occurrence_recent"
        if not items and self.widen:
            items = self._query(product_id)
            source = "co_occurrence_retained"
        if not items:
            source = "no_pairs"
        self.sources[source] += 1
        self.cache[product_id] = items
        return items


def replayed_window():
    """What the pipeline was last fed, as scripts/replay.py recorded it."""
    try:
        from src.common import mongo
        db = mongo.client(config.MONGO_URI)[config.MONGO_DB]
        return db["replay_runs"].find_one(sort=[("finished_at", -1)])
    except Exception:                                   # noqa: BLE001
        return None                                     # no record: cannot check


def check_against_replay(run, start_day: str, train_days: float) -> str | None:
    """The training window must be the window that was replayed.

    Evaluating `--train-days 7` against a stack loaded with a 30-day replay
    scores the pipeline on days it was trained on: measured here at 35.1%
    against a truthful 17.4%. Nothing in the data reveals it, because the
    pipeline's rows carry replay-clock timestamps rather than dataset dates.
    """
    if not run:
        return None
    if run.get("start_day") == start_day and float(run.get("days", 0)) == train_days:
        return None
    return (f"the pipeline holds a {run.get('days')}-day replay from "
            f"{run.get('start_day')}, but this asks for {train_days:g} days "
            f"from {start_day}. Testing on days the pipeline was trained on "
            f"inflates the score; replay that window first, or pass "
            f"--allow-window-mismatch to measure anyway.")


def load_categories(path: Path) -> dict[int, str]:
    """Item id to category, from the catalogue scripts/replay.py writes.

    Build it for every item with `python scripts/replay.py --dry-run --days 140`.
    A catalogue from a 30-day replay only knows that month's items, and the
    rest fall back to the overall bestsellers, which understates the baseline.
    """
    if not path.is_file():
        return {}
    return {int(p["id"]): p["category"]
            for p in json.loads(path.read_text(encoding="utf-8"))}


def table(rows: list[tuple[str, str]]) -> str:
    width = max(len(name) for name, _ in rows)
    return "\n".join(f"  {name:<{width}}  {value}" for name, value in rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, default=RAW / "events.csv")
    parser.add_argument("--catalog", type=Path, default=ROOT / "data" / "catalog_retailrocket.json",
                        help="item categories, for the category-bestseller baseline")
    parser.add_argument("--train-days", type=float, default=30.0,
                        help="must match the replay's --days; the evaluation\n     reads the manifest replay.py wrote and refuses any other window")
    parser.add_argument("--test-days", type=float, default=7.0)
    parser.add_argument("--start", type=str, default=None,
                        help="first training day, YYYY-MM-DD (must match the replay)")
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--limit", type=int, default=3000,
                        help="sample this many test cases (0 = all)")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--source", choices=["api", "mongo"], default="api")
    parser.add_argument("--score-by", choices=["affinity", "lift", "pmi"],
                        default="affinity",
                        help="how the API should rank co-occurring products")
    parser.add_argument("--allow-window-mismatch", action="store_true",
                        help="evaluate even when the training window is not "
                             "the one that was replayed (the result will "
                             "include data the pipeline was trained on)")
    parser.add_argument("--strict-lookback", action="store_true",
                        help="with --source mongo, answer only from the recent "
                             "lookback instead of widening to everything "
                             "retained when it is empty")
    parser.add_argument("--no-report", action="store_true")
    args = parser.parse_args()

    if not args.events.is_file():
        log.error("%s not found - run scripts/fetch_dataset.py first", args.events)
        return 2

    started = time.time()
    log.info("splitting %s: %s training days, %s test days",
             args.events, args.train_days, args.test_days)
    train_views, train_events, visits, begin, (first, newest) = split(
        args.events, args.train_days, args.test_days, args.start)
    log.info("dataset spans %s to %s; training from %s for %g days, testing "
             "the %g days after that",
             day_of(first), day_of(newest), day_of(begin), args.train_days,
             args.test_days)
    if not visits:
        log.error("no test traffic between day %g and day %g after %s. The "
                  "dataset ends on %s - ask for fewer days, or move --start.",
                  args.train_days, args.train_days + args.test_days,
                  day_of(begin), day_of(newest))
        return 1

    problem = check_against_replay(replayed_window(), day_of(begin),
                                   args.train_days)
    if problem and not args.allow_window_mismatch:
        log.error("%s", problem)
        return 2
    if problem:
        log.warning("MEASURING ANYWAY: %s", problem)

    cases = ev.test_cases(visits)
    if args.limit and len(cases) > args.limit:
        random.Random(args.seed).shuffle(cases)
        cases = cases[:args.limit]
    log.info("train: %s events, %s products viewed | test: %s visits -> %s cases",
             f"{train_events:,}", f"{len(train_views):,}", f"{len(visits):,}",
             f"{len(cases):,}")

    top = ev.bestsellers(train_views, args.k)
    baseline = ev.evaluate(f"bestsellers (top {args.k} of the training days)",
                           cases, lambda _product: top, args.k)
    categories = load_categories(args.catalog)
    by_category = None
    if categories:
        known = sum(c.query in categories for c in cases) / max(1, len(cases))
        log.info("catalogue %s: %s items, %.1f%% of query products have a category",
                 args.catalog, f"{len(categories):,}", 100 * known)
        by_category = ev.evaluate(
            "category bestsellers", cases,
            ev.category_bestsellers(train_views, categories, args.k), args.k)
    else:
        log.warning("no catalogue at %s, so the category-bestseller baseline is "
                    "not measured. Build one: python scripts/replay.py --dry-run "
                    "--days 140", args.catalog)

    recommender = (ApiRecommender(config.API_BASE_URL, args.k, args.score_by)
                   if args.source == "api"
                   else MongoRecommender(args.k, config.PAIR_LOOKBACK_MINUTES,
                                         widen=not args.strict_lookback))
    model = ev.evaluate(f"pipeline ({args.source})", cases, recommender, args.k)
    ratio = ev.lift(model, baseline)

    # A serving layer that does not recognise the products is not a model
    # scoring zero - it is a misconfiguration, and reporting 0.000% as a
    # result would be worse than reporting nothing.
    unknown = recommender.sources.get("unknown_product", 0)
    if model.cases and unknown > model.cases / 2:
        log.error("the API did not recognise %s of %s query products. It is "
                  "serving the demo catalogue, so every real item is a 404.\n"
                  "Point the stack at the catalogue the replay built, then "
                  "run this again:\n"
                  "    CATALOG_FILE=data/catalog_retailrocket.json "
                  "docker compose up -d --force-recreate api dashboard\n"
                  "PowerShell: $env:CATALOG_FILE = "
                  "\"data/catalog_retailrocket.json\" on the line before.",
                  unknown, model.cases)
        return 1

    print()
    print(table([
        ("test cases", f"{model.cases:,}"),
        (f"hit-rate@{args.k}, pipeline",
         f"{model.hit_rate:.3%}  ({model.hits:,} hits)"),
        (f"hit-rate@{args.k}, bestsellers",
         f"{baseline.hit_rate:.3%}  ({baseline.hits:,} hits)"),
        ("lift over the baseline",
         f"{ratio:.2f}x" if ratio else "baseline scored zero"),
        (f"hit-rate@{args.k}, category bestsellers",
         f"{by_category.hit_rate:.3%}  ({by_category.hits:,} hits)"
         if by_category else "not measured (no catalogue)"),
        ("difference from category bestsellers",
         f"{100 * (model.hit_rate - by_category.hit_rate):+.2f} points"
         if by_category else "not measured"),
        ("answered (coverage)", f"{model.coverage:.1%}"),
        ("any-product hit-rate",
         f"{model.any_hit_rate:.3%} vs {baseline.any_hit_rate:.3%}"),
        ("answer sources", ", ".join(f"{k}={v:,}" for k, v in
                                     sorted(recommender.sources.items()))),
    ]))
    print()

    summary = {
        "measured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataset": "retailrocket",
        "train_days": args.train_days, "test_days": args.test_days,
        "train_start": day_of(begin), "k": args.k, "source": args.source,
        "score_by": args.score_by if args.source == "api" else "pair_count",
        "cases": model.cases,
        "pipeline": {"hit_rate": model.hit_rate, "hits": model.hits,
                     "any_hit_rate": model.any_hit_rate,
                     "coverage": model.coverage},
        "baseline": {"hit_rate": baseline.hit_rate, "hits": baseline.hits,
                     "any_hit_rate": baseline.any_hit_rate},
        "lift": ratio,
        "category_baseline": ({"hit_rate": by_category.hit_rate,
                               "hits": by_category.hits,
                               "any_hit_rate": by_category.any_hit_rate}
                              if by_category else None),
        "points_over_category": (round(100 * (model.hit_rate - by_category.hit_rate), 2)
                                 if by_category else None),
        "sources": dict(recommender.sources),
        "seconds": round(time.time() - started, 1),
    }
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / "evaluation.json").write_text(json.dumps(summary, indent=2),
                                             encoding="utf-8")

    if not args.no_report:
        body = "\n".join([
            f"_{summary['measured_at']}, k={args.k}, source={args.source}, "
            f"{model.cases:,} test cases_",
            "",
            f"| Model | hit-rate@{args.k} | Hits | Coverage |",
            "|---|---:|---:|---:|",
            f"| **Pipeline** (co-occurrence, then category fill, served by the API) | "
            f"**{model.hit_rate:.2%}** | {model.hits:,} | {model.coverage:.0%} |",
            *([f"| Category bestsellers (top {args.k} of the query's category) | "
               f"{by_category.hit_rate:.2%} | {by_category.hits:,} | 100% |"]
              if by_category else []),
            f"| Bestsellers (top {args.k} of the training days) | "
            f"{baseline.hit_rate:.2%} | {baseline.hits:,} | 100% |",
            "",
            f"Training: {args.train_days:g} days from {day_of(begin)} "
            f"({train_events:,} events), replayed through Kafka. "
            f"Test: the following {args.test_days:g} day(s), "
            f"{len(visits):,} visits, never seen by the pipeline.",
            "",
            (f"Against the category bestsellers the pipeline is "
             f"**{100 * (model.hit_rate - by_category.hit_rate):+.2f} points**, "
             f"and it is {ratio:.2f}x the overall bestsellers."
             if by_category and ratio else
             f"The pipeline is **{ratio:.2f}x** the overall bestsellers. The "
             f"category baseline was not measured (no catalogue)."
             if ratio else "The baseline scored zero on these cases."),
        ])
        if not REPORT.is_file():
            REPORT.parent.mkdir(parents=True, exist_ok=True)
            REPORT.write_text(REPORT_HEADER, encoding="utf-8")
        bench.update_section("hitrate", body, path=str(REPORT))
        log.info("wrote %s and %s", REPORT, RESULTS / "evaluation.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
