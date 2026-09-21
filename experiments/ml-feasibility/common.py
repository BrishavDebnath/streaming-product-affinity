"""Shared pieces for the ML feasibility experiments.

Everything here is offline and reads the RetailRocket export directly. It is
NOT the pipeline: co-occurrence is approximated per visit with the pipeline's
2-minute co-view gap and minimum pair count, but without Spark's one-minute
windows. So compare methods against each other inside these scripts, and
never against the published numbers in the README, which come from the live
API.

Needs `pip install -r requirements-ml.txt` and `python scripts/fetch_dataset.py`.
"""

import collections
import json
import math
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.data import evaluation as ev  # noqa: E402
from src.data import retailrocket as rr  # noqa: E402

EVENTS = ROOT / "data" / "raw" / "events.csv"
TREE = ROOT / "data" / "raw" / "category_tree.csv"
CATALOG = ROOT / "data" / "catalog_retailrocket.json"

DAY = 86400
K = 10          # hit-rate@10, as published
GAP = 120       # the pipeline's CO_VIEW_GAP in seconds
MIN_PAIRS = 3   # the pipeline's MIN_PAIR_COUNT
SEED = 7        # the seed scripts/evaluate.py samples its 3,000 cases with

events = list(rr.read_events(str(EVENTS)))
first = min(e.at for e in events)
catalog = {p["id"]: p["category"] for p in json.loads(CATALOG.read_text(encoding="utf-8"))}
parent: dict[str, str | None] = {}
for line in TREE.read_text(encoding="utf-8").splitlines()[1:]:
    child, par = line.split(",")
    parent[f"cat-{child}"] = f"cat-{par}" if par else None


def window(start_day, end_day):
    """Events from day `start_day` (inclusive) to `end_day` (exclusive)."""
    lo, hi = first + start_day * DAY, first + end_day * DAY
    return [e for e in events if lo <= e.at < hi]


def visits_in(start_day, end_day):
    return list(rr.sessionise(rr.sort_events(window(start_day, end_day))))


def cases_for(start_day, end_day):
    """The pipeline's test cases: first item of a visit, then the next one."""
    return ev.test_cases(visits_in(start_day, end_day))


def co_view_pairs(visits):
    """Pair counts the way the pipeline counts them: same visit, within GAP."""
    pairs = collections.Counter()
    for visit in visits:
        evs = visit.events
        for i, a in enumerate(evs):
            for b in evs[i + 1:]:
                if b.at - a.at > GAP:
                    break
                if a.item != b.item:
                    pairs[tuple(sorted((a.item, b.item)))] += 1
    return pairs


def neighbours(pairs, min_pairs=MIN_PAIRS):
    nbr = collections.defaultdict(list)
    for (x, y), n in pairs.items():
        if n >= min_pairs:
            nbr[x].append((n, y))
            nbr[y].append((n, x))
    for x in nbr:
        nbr[x].sort(reverse=True)
    return nbr


class Features:
    """Everything a ranker may know, built from ONE training window only.

    Building it from days 0-23 and labelling with days 23-30, then building
    it again from days 0-30 for the test week, is what keeps the test week
    unseen. The same separation the replay manifest enforces for the pipeline.
    """

    def __init__(self, evs):
        from gensim.models import Word2Vec

        self.visits = list(rr.sessionise(rr.sort_events(evs)))
        end = max(e.at for e in evs)
        self.views = collections.Counter(e.item for e in evs if e.event_type == "view")
        self.recent = collections.Counter(e.item for e in evs
                                          if e.event_type == "view" and e.at >= end - 3 * DAY)
        self.carts = collections.Counter(e.item for e in evs if e.event_type == "add_to_cart")
        self.buys = collections.Counter(e.item for e in evs if e.event_type == "purchase")
        self.pc = co_view_pairs(self.visits)
        self.nbr = neighbours(self.pc)
        self.item_pairs = collections.Counter()
        for (x, y), n in self.pc.items():
            self.item_pairs[x] += n
            self.item_pairs[y] += n
        self.total_pairs = sum(self.pc.values()) or 1
        by_cat = collections.defaultdict(collections.Counter)
        for item, n in self.views.items():
            by_cat[catalog.get(item)][item] = n
        self.cat_top = {c: [i for i, _ in cnt.most_common(30)] for c, cnt in by_cat.items()}
        self.cat_rank = {i: r for lst in self.cat_top.values() for r, i in enumerate(lst)}
        sentences = [[str(i) for i in v.items] for v in self.visits if len(v.items) >= 2]
        self.w2v = Word2Vec(sentences, vector_size=64, window=5, min_count=2, sg=1,
                            negative=10, epochs=15, workers=2, seed=SEED, sample=1e-4)
        self.global_top = [i for i, _ in self.views.most_common(10)]

    def similar(self, q, n=30):
        key = str(q)
        if key not in self.w2v.wv:
            return []
        return [int(x) for x, _ in self.w2v.wv.most_similar(key, topn=n)]

    def candidates(self, q):
        co = [y for _, y in self.nbr.get(q, [])[:30]]
        cat = self.cat_top.get(catalog.get(q), [])[:30]
        i2v = self.similar(q)
        out = []
        for source in (co, cat, i2v, self.global_top):
            for x in source:
                if x != q and x not in out:
                    out.append(x)
        return out, {x: r for r, x in enumerate(co)}, {x: r for r, x in enumerate(i2v)}

    def rows(self, q):
        cands, co_rank, i2v_rank = self.candidates(q)
        q_views, q_cat = self.views.get(q, 0), catalog.get(q)
        wv = self.w2v.wv
        feats = []
        for c in cands:
            n = self.pc.get(tuple(sorted((q, c))), 0)
            ip_q, ip_c = self.item_pairs.get(q, 0), self.item_pairs.get(c, 0)
            lift = (n * self.total_pairs / (ip_q * ip_c)) if n and ip_q and ip_c else 0.0
            cos = float(wv.similarity(str(q), str(c))) if str(q) in wv and str(c) in wv else 0.0
            c_cat, c_views = catalog.get(c), self.views.get(c, 0)
            feats.append([
                n, math.log1p(n), n / (ip_q + 1), math.log1p(lift),
                co_rank.get(c, 99), i2v_rank.get(c, 99), cos,
                float(c_cat == q_cat and q_cat is not None),
                float(c_cat is not None and q_cat is not None
                      and parent.get(c_cat) == parent.get(q_cat)),
                self.cat_rank.get(c, 99) if c_cat == q_cat else 99,
                math.log1p(c_views), math.log1p(self.recent.get(c, 0)),
                (self.carts.get(c, 0) + 1) / (c_views + 20), math.log1p(self.buys.get(c, 0)),
                math.log1p(q_views), float(n >= MIN_PAIRS),
            ])
        return cands, feats


FEATURE_NAMES = ["pair_n", "log_pair_n", "pair_over_q", "log_lift", "co_rank", "i2v_rank",
                 "i2v_cos", "same_cat", "same_parent", "cat_rank", "log_views", "log_recent",
                 "cart_rate", "log_buys", "log_q_views", "passes_min_pairs"]


def training_rows(features, cases):
    """Rows for LambdaRank. Cases whose target is not a candidate are skipped:
    there is no positive to learn from."""
    X, y, groups = [], [], []
    for case in cases:
        cands, feats = features.rows(case.query)
        if not cands or case.target not in cands:
            continue
        X += feats
        y += [int(x == case.target) for x in cands]
        groups.append(len(cands))
    return np.array(X, dtype=float), np.array(y), groups


def train_ranker(features, cases):
    import lightgbm as lgb

    X, y, groups = training_rows(features, cases)
    model = lgb.LGBMRanker(objective="lambdarank", n_estimators=300, learning_rate=0.05,
                           num_leaves=31, min_child_samples=50, subsample=0.8,
                           subsample_freq=1, colsample_bytree=0.8, random_state=SEED,
                           verbose=-1)
    model.fit(X, y, group=groups)
    return model, len(groups), len(y)


def ranked_by(model, features):
    def recommend(q):
        cands, feats = features.rows(q)
        if not cands:
            return []
        scores = model.predict(np.array(feats, dtype=float))
        return [cands[i] for i in np.argsort(-scores)][:K + 1]
    return recommend


def co_then_category(features):
    """The strongest non-ML method found: co-occurrence, then the query
    category's bestsellers to fill the empty slots."""
    def recommend(q):
        out = []
        for x in ([y for _, y in features.nbr.get(q, [])]
                  + features.cat_top.get(catalog.get(q), [])):
            if x != q and x not in out:
                out.append(x)
        return out[:K + 1]
    return recommend
