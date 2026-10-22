"""
The one feature function for the learned re-ranker.

The trainer and the API both call `Snapshot.from_mongo` and `rows`. If the two
computed features separately, the offline gain could vanish in production
with no error anywhere (training/serving skew), so there is no second copy.

Everything here reads the aggregates Spark already writes:

    trending        per-product counts per window, split by event type
    product_pairs   co-occurrence counts per window, mirrored

plus two files that ship next to the model: item2vec vectors and the
category tree's parent links.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import numpy as np

from src.common import config

# Order matters: the model's columns are these, in this order. The API
# refuses a model whose metadata lists anything else.
FEATURE_NAMES = (
    "pair_n", "log_pair_n", "pair_over_q", "log_lift", "co_rank", "i2v_rank",
    "i2v_cos", "same_cat", "same_parent", "cat_rank", "log_views", "log_recent",
    "cart_rate", "log_buys", "log_q_views", "passes_min_pairs",
)

PER_SOURCE = 30      # candidates taken from each source
GLOBAL_TOP = 10      # overall bestsellers, the last source
ABSENT_RANK = 99     # rank feature for "not in that source's list"
# "Recent" is the last tenth of the window: three days of a 30-day replay.
RECENT_SHARE = 0.1


class Embeddings:
    """Item2vec vectors with a cosine lookup. No gensim needed to serve."""

    def __init__(self, ids: np.ndarray, vectors: np.ndarray):
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        self.ids = ids.astype(np.int64)
        self.unit = (vectors / np.where(norms == 0, 1, norms)).astype(np.float32)
        self.index = {int(i): n for n, i in enumerate(self.ids)}

    @classmethod
    def load(cls, path) -> Embeddings:
        data = np.load(path)
        return cls(data["ids"], data["vectors"])

    def similar(self, item: int, n: int = PER_SOURCE) -> list[int]:
        row = self.index.get(item)
        if row is None:
            return []
        scores = self.unit @ self.unit[row]
        scores[row] = -np.inf
        top = np.argpartition(-scores, min(n, len(scores) - 1))[:n]
        top = top[np.argsort(-scores[top])]
        return [int(self.ids[i]) for i in top]

    def cosine(self, a: int, b: int) -> float:
        ra, rb = self.index.get(a), self.index.get(b)
        if ra is None or rb is None:
            return 0.0
        return float(self.unit[ra] @ self.unit[rb])


@dataclass
class Snapshot:
    """The whole-window aggregates, built once and reused for every query."""

    views: dict[int, float]
    recent: dict[int, float]
    carts: dict[int, float]
    buys: dict[int, float]
    item_pairs: dict[int, float]
    total_pairs: float
    category_of: Callable[[int], str | None]
    parent: dict[str, str | None]
    embeddings: Embeddings | None
    cat_top: dict[str, list[int]] = field(default_factory=dict)
    cat_rank: dict[int, int] = field(default_factory=dict)
    global_top: list[int] = field(default_factory=list)

    def __post_init__(self):
        by_cat: dict[str, Counter] = defaultdict(Counter)
        for item, n in self.views.items():
            category = self.category_of(item)
            if category:
                by_cat[category][item] = n
        self.cat_top = {c: [i for i, _ in sorted(cnt.items(), key=lambda kv: (-kv[1], kv[0]))
                            [:PER_SOURCE]] for c, cnt in by_cat.items()}
        self.cat_rank = {i: r for top in self.cat_top.values() for r, i in enumerate(top)}
        self.global_top = [i for i, _ in sorted(self.views.items(),
                                                key=lambda kv: (-kv[1], kv[0]))[:GLOBAL_TOP]]

    @classmethod
    def from_mongo(cls, db, cutoff: datetime | None, *,
                   category_of: Callable[[int], str | None],
                   parent: dict[str, str | None],
                   embeddings: Embeddings | None) -> Snapshot:
        """Read the aggregates for every window from `cutoff` on."""
        match = {"window_start": {"$gte": cutoff}} if cutoff is not None else {}
        trending = db[config.COLL_TRENDING]
        oldest = trending.find_one(match, {"window_start": 1}, sort=[("window_start", 1)])
        newest = trending.find_one(match, {"window_start": 1}, sort=[("window_start", -1)])
        recent_from = None
        if oldest and newest:
            span = newest["window_start"] - oldest["window_start"]
            recent_from = newest["window_start"] - span * RECENT_SHARE

        # Rows written before the per-type split existed have no `views`;
        # counting every event as a view is the closest honest reading.
        views_expr = {"$ifNull": ["$views", "$event_count"]}
        rows = trending.aggregate([
            {"$match": match},
            {"$group": {
                "_id": "$product_id",
                "views": {"$sum": views_expr},
                "recent": {"$sum": {"$cond": [{"$gte": ["$window_start", recent_from]},
                                              views_expr, 0]}}
                if recent_from is not None else {"$sum": 0},
                "carts": {"$sum": {"$ifNull": ["$carts", 0]}},
                "buys": {"$sum": {"$ifNull": ["$purchases", 0]}},
            }},
        ])
        views, recent, carts, buys = {}, {}, {}, {}
        for r in rows:
            pid = r["_id"]
            views[pid], recent[pid] = float(r["views"]), float(r["recent"])
            carts[pid], buys[pid] = float(r["carts"]), float(r["buys"])

        item_pairs = {r["_id"]: float(r["n"]) for r in db[config.COLL_PAIRS].aggregate([
            {"$match": match},
            {"$group": {"_id": "$product_id", "n": {"$sum": "$pair_count"}}},
        ])}
        # Every pair is stored twice (mirrored), so half the sum is the number
        # of co-views.
        total = sum(item_pairs.values()) / 2
        return cls(views, recent, carts, buys, item_pairs, total or 1.0,
                   category_of, parent, embeddings)


def anchored_cutoff(db) -> datetime | None:
    """The start of the pair lookback, anchored on the newest data.

    Anchored on the data rather than on the clock, so the trainer and the API
    read the same windows however long after a replay they run.
    """
    # Sorted on window_start because that is what the trending index covers;
    # sorting on window_end scanned every row, ~100 ms per request after a
    # replayed month. Windows are fixed-length, so the newest start has the
    # newest end.
    row = db[config.COLL_TRENDING].find_one({}, {"window_end": 1},
                                            sort=[("window_start", -1)])
    if not row or not row.get("window_end"):
        return None
    newest = row["window_end"]
    if newest.tzinfo is None:
        newest = newest.replace(tzinfo=timezone.utc)
    newest = min(newest, datetime.now(timezone.utc))
    return newest - timedelta(minutes=config.PAIR_LOOKBACK_MINUTES)


def neighbours_from_mongo(db, product_id: int,
                          cutoff: datetime | None) -> dict[int, float]:
    """Every product co-viewed with `product_id`, with no minimum count.

    The minimum (MIN_PAIR_COUNT) still decides what the plain co-occurrence
    ranking shows. Candidates do without it: offline, dropping it raised the
    share of cases with the right answer among the candidates from 43.8% to
    49.2%, and `passes_min_pairs` lets the model weigh the thin ones.
    """
    match: dict = {"product_id": product_id}
    if cutoff is not None:
        match["window_start"] = {"$gte": cutoff}
    rows = db[config.COLL_PAIRS].aggregate([
        {"$match": match},
        {"$group": {"_id": "$related_product_id", "n": {"$sum": "$pair_count"}}},
    ])
    return {r["_id"]: float(r["n"]) for r in rows if r["_id"] != product_id}


def candidates(query: int, nbrs: dict[int, float], snap: Snapshot):
    """Co-occurrence, then the category, then item2vec, then bestsellers.

    Returns the candidates in that order, plus each one's rank within the
    co-occurrence and item2vec lists, and the source that first proposed it.
    """
    co = [i for i, _ in sorted(nbrs.items(), key=lambda kv: (-kv[1], kv[0]))[:PER_SOURCE]]
    cat = snap.cat_top.get(snap.category_of(query) or "", [])
    i2v = snap.embeddings.similar(query) if snap.embeddings else []
    out: list[int] = []
    origin: dict[int, str] = {}
    for name, source in (("co_occurrence", co), ("category", cat),
                         ("item2vec", i2v), ("bestsellers", snap.global_top)):
        for item in source:
            if item != query and item not in origin:
                out.append(item)
                origin[item] = name
    return (out, {x: r for r, x in enumerate(co)}, {x: r for r, x in enumerate(i2v)},
            origin)


def rows(query: int, nbrs: dict[int, float], snap: Snapshot):
    """The candidates for `query` and one feature row per candidate."""
    cands, co_rank, i2v_rank, origin = candidates(query, nbrs, snap)
    q_cat = snap.category_of(query)
    q_views = snap.views.get(query, 0.0)
    ip_q = snap.item_pairs.get(query, 0.0)
    feats = []
    for c in cands:
        n = nbrs.get(c, 0.0)
        ip_c = snap.item_pairs.get(c, 0.0)
        lift = n * snap.total_pairs / (ip_q * ip_c) if n and ip_q and ip_c else 0.0
        c_cat = snap.category_of(c)
        c_views = snap.views.get(c, 0.0)
        same_cat = c_cat is not None and c_cat == q_cat
        feats.append([
            n, math.log1p(n), n / (ip_q + 1), math.log1p(lift),
            co_rank.get(c, ABSENT_RANK), i2v_rank.get(c, ABSENT_RANK),
            snap.embeddings.cosine(query, c) if snap.embeddings else 0.0,
            float(same_cat),
            float(c_cat is not None and q_cat is not None
                  and snap.parent.get(c_cat) is not None
                  and snap.parent.get(c_cat) == snap.parent.get(q_cat)),
            snap.cat_rank.get(c, ABSENT_RANK) if same_cat else ABSENT_RANK,
            math.log1p(c_views), math.log1p(snap.recent.get(c, 0.0)),
            (snap.carts.get(c, 0.0) + 1) / (c_views + 20),
            math.log1p(snap.buys.get(c, 0.0)),
            math.log1p(q_views), float(n >= config.MIN_PAIR_COUNT),
        ])
    return cands, np.array(feats, dtype=float).reshape(len(cands), len(FEATURE_NAMES)), origin
