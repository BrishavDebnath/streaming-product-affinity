"""The re-ranker's gain on three separate periods, with a paired bootstrap
95% interval. One split can be luck; three that agree are not."""
import numpy as np

from common import (
    Features,
    K,
    cases_for,
    co_then_category,
    ranked_by,
    train_ranker,
    window,
)

for offset in (0, 30, 60):
    model, _, _ = train_ranker(Features(window(offset, offset + 23)),
                               cases_for(offset + 23, offset + 30))
    features = Features(window(offset, offset + 30))
    cases = cases_for(offset + 30, offset + 37)

    def hits(recommend, cases=cases):
        return np.array([c.target in [x for x in recommend(c.query) if x != c.query][:K]
                         for c in cases], dtype=float)

    base, ranked = hits(co_then_category(features)), hits(ranked_by(model, features))
    rng = np.random.default_rng(7)
    draws = [(ranked[i] - base[i]).mean()
             for i in (rng.integers(0, len(base), len(base)) for _ in range(1000))]
    lo, hi = np.percentile(draws, [2.5, 97.5])
    print(f"days {offset} to {offset + 37}: {len(cases):,} cases, best non-ML {base.mean():.2%}, "
          f"re-ranker {ranked.mean():.2%}, gain {ranked.mean() - base.mean():+.2%} "
          f"(95% interval {lo:+.2%} to {hi:+.2%})")
