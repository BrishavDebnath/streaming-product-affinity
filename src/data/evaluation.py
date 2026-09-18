"""
Offline evaluation: does the pipeline's answer beat the obvious baseline?

The question a recommender has to answer is "given what this shopper just
looked at, what will they look at next". So each test case takes a real
visit from a period the pipeline never saw, hands the model the FIRST product
of that visit, and checks whether the product the shopper actually went to
next is in the top 10 that came back.

The baseline is the bestseller list: the ten most-viewed products of the
training period, the same ten for everybody. It is what a shop does when it
has no recommender at all, and it is a genuinely hard baseline to beat -
popular products are popular for everyone. A co-occurrence model that cannot
beat it is not earning its keep.

Pure functions only: no Mongo, no HTTP. `scripts/evaluate.py` supplies the
recommendations, this decides what the numbers mean.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable
from typing import NamedTuple


class Case(NamedTuple):
    """One test: the shopper saw `query`, then went to `target`."""

    session: str
    query: int
    target: int
    later: tuple[int, ...]       # every distinct product after `query`


def test_cases(visits: Iterable) -> list[Case]:
    """One case per visit that has at least two distinct products.

    Single-product visits are dropped: there is nothing to predict. Taking one
    case per visit rather than one per step keeps long visits from dominating
    the result.
    """
    cases = []
    for visit in visits:
        items = visit.items
        if len(items) < 2:
            continue
        cases.append(Case(visit.session, items[0], items[1], tuple(items[1:])))
    return cases


def bestsellers(view_counts: dict[int, int] | Counter, k: int) -> list[int]:
    """The k most-viewed products, ties broken by id so runs are repeatable."""
    return [item for item, _ in
            sorted(view_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:k]]


class Result(NamedTuple):
    name: str
    cases: int
    hits: int
    any_hits: int                # the target OR any later product in the visit
    answered: int                # cases where the model returned anything

    @property
    def hit_rate(self) -> float:
        return self.hits / self.cases if self.cases else 0.0

    @property
    def any_hit_rate(self) -> float:
        return self.any_hits / self.cases if self.cases else 0.0

    @property
    def coverage(self) -> float:
        return self.answered / self.cases if self.cases else 0.0


def evaluate(name: str, cases: Iterable[Case],
             recommend: Callable[[int], list[int]], k: int = 10) -> Result:
    """Score a recommender over the cases.

    `recommend` returns products for one query product, best first; an empty
    list means "no answer", which counts as a miss and lowers coverage rather
    than being quietly skipped - a model that answers 5% of queries perfectly
    is not a good model.
    """
    cases = list(cases)
    hits = any_hits = answered = 0
    for case in cases:
        top = [item for item in recommend(case.query) if item != case.query][:k]
        if top:
            answered += 1
        if case.target in top:
            hits += 1
        if any(item in top for item in case.later):
            any_hits += 1
    return Result(name, len(cases), hits, any_hits, answered)


def lift(model: Result, baseline: Result) -> float | None:
    """How many times better than the baseline. None if the baseline is 0."""
    if not baseline.hit_rate:
        return None
    return model.hit_rate / baseline.hit_rate
