"""
Ranking scores for co-occurrence pairs.

Pure functions, no Spark and no Mongo, so the maths is unit-testable on its
own. The API imports these; nothing here touches I/O.

Why this module exists
----------------------
Raw co-occurrence count answers "how often were A and B seen together", which
is dominated by how popular A and B are individually. The most-viewed product
co-occurs with everything, so count-ranked related products degenerate into a
bestseller list — the exact symptom seen in production: for a MacBook Air, the
top "related products" were the two most-trending phones.

Lift divides out that popularity:

    lift(A,B) = P(A,B) / (P(A) * P(B))

    > 1  seen together MORE than independence predicts  -> real affinity
    = 1  exactly what chance predicts                   -> no signal
    < 1  seen together LESS than chance                 -> substitutes

A laptop sleeve is rarely viewed on its own but almost always viewed WITH a
laptop, so it has a low count and a high lift. That is the pair worth
showing, and count-ranking buries it.
"""

import math
from collections.abc import Iterable


def lift(pair_count: float, count_a: float, count_b: float,
         total: float) -> float | None:
    """
    Lift for one pair. Returns None when it is undefined — either product
    unseen, or an empty corpus. Callers must handle None rather than
    silently treating it as zero.
    """
    if total <= 0 or count_a <= 0 or count_b <= 0:
        return None
    p_ab = pair_count / total
    p_a = count_a / total
    p_b = count_b / total
    denominator = p_a * p_b
    if denominator <= 0:
        return None
    return p_ab / denominator


def pmi(pair_count: float, count_a: float, count_b: float,
        total: float) -> float | None:
    """
    Pointwise mutual information — log of lift. Same ranking as lift, but a
    symmetric scale around 0 that is easier to threshold and to average.
    """
    value = lift(pair_count, count_a, count_b, total)
    if value is None or value <= 0:
        return None
    return math.log2(value)


def score_pairs(pairs: Iterable[dict],
                product_counts: dict[int, float],
                total: float,
                anchor_id: int,
                method: str = "affinity") -> list[dict]:
    """
    Attach a ranking score to each candidate pair and sort best-first.

    `pairs`          rows with related_product_id, pair_count, affinity
    `product_counts` per-product interaction counts over the same window range
    `total`          total interactions over that range
    `method`         "affinity" (weighted count) or "lift" or "pmi"

    Pairs whose score is undefined keep their affinity ordering and are
    flagged, rather than being dropped — a product with no count data yet is
    a cold-start case, not an error.
    """
    if method not in {"affinity", "lift", "pmi"}:
        raise ValueError(f'method must be affinity, lift or pmi; got {method!r}')

    count_a = product_counts.get(anchor_id, 0.0)
    scored = []
    for row in pairs:
        related = row["related_product_id"]
        count_b = product_counts.get(related, 0.0)
        row = dict(row)

        row["lift"] = lift(row["pair_count"], count_a, count_b, total)
        row["pmi"] = pmi(row["pair_count"], count_a, count_b, total)

        if method == "affinity":
            row["score"] = row["affinity"]
        else:
            value = row[method]
            # Undefined score: fall back to affinity so the row still ranks,
            # and say so explicitly.
            row["score"] = value if value is not None else row["affinity"]
            row["score_undefined"] = value is None

        for key in ("lift", "pmi"):
            if row[key] is not None:
                row[key] = round(row[key], 4)
        scored.append(row)

    # Rows with a defined score rank above rows that fell back to affinity.
    # Affinity and lift are on different scales, so letting a fallback
    # affinity of 2.0 outrank a real lift of 0.4 was comparing unlike numbers.
    scored.sort(key=lambda r: (not r.get("score_undefined", False), r["score"]),
                reverse=True)
    return scored
