"""A learned re-ranker over multi-source candidates, plus a purchase-intent
check. Trained on days 23-30 with features from days 0-23, tested on days
30-37 with features from days 0-30: the test week is never seen."""
import math
import random

import lightgbm as lgb
import numpy as np
from sklearn.metrics import average_precision_score, roc_auc_score

from common import (
    FEATURE_NAMES,
    SEED,
    Features,
    K,
    cases_for,
    catalog,
    co_then_category,
    ev,
    ranked_by,
    train_ranker,
    visits_in,
    window,
)

train_features = Features(window(0, 23))
model, n_cases, n_rows = train_ranker(train_features, cases_for(23, 30))
print(f"ranker trained on {n_cases:,} cases ({n_rows:,} candidate rows)")

test_features = Features(window(0, 30))
cases = cases_for(30, 37)
sample = list(cases)
random.Random(SEED).shuffle(sample)
sample = sample[:3000]
for name, recommend in (("co-occurrence, then category bestsellers", co_then_category(test_features)),
                        ("learned re-ranker (LightGBM LambdaRank)", ranked_by(model, test_features))):
    for label, cs in (("all", cases), ("3k", sample)):
        r = ev.evaluate(name, cs, recommend, K)
        print(f"{name:<44} {label}: hit@10 {r.hit_rate:.2%}  later-in-visit {r.any_hit_rate:.2%}  "
              f"coverage {r.coverage:.1%}")
ceiling = sum(c.target in test_features.candidates(c.query)[0] for c in cases) / len(cases)
print(f"candidate recall (target anywhere among the candidates): {ceiling:.1%}")
gains = sorted(zip(model.booster_.feature_importance("gain"), FEATURE_NAMES, strict=True), reverse=True)
print("most useful features:", [name for _, name in gains[:8]])


def intent_rows(features, visits, n_first=3):
    """Predict an add-to-cart or purchase later in the visit from its first
    three views. Visits that already converted in those three are skipped."""
    X, y = [], []
    for visit in visits:
        head, tail = visit.events[:n_first], visit.events[n_first:]
        if not tail or any(e.event_type != "view" for e in head):
            continue
        items = [e.item for e in head]
        span = head[-1].at - head[0].at
        X.append([len(set(items)), span, span / max(1, len(head) - 1),
                  np.mean([math.log1p(features.views.get(i, 0)) for i in items]),
                  max((features.carts.get(i, 0) + 1) / (features.views.get(i, 0) + 20) for i in items),
                  max(math.log1p(features.buys.get(i, 0)) for i in items),
                  len({catalog.get(i) for i in items}), float(len(set(items)) < len(items))])
        y.append(int(any(e.event_type in ("add_to_cart", "purchase") for e in tail)))
    return np.array(X, dtype=float), np.array(y)


Xa, ya = intent_rows(train_features, visits_in(23, 30))
Xb, yb = intent_rows(test_features, visits_in(30, 37))
p = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.05, num_leaves=31, random_state=SEED,
                       verbose=-1).fit(Xa, ya).predict_proba(Xb)[:, 1]
top = np.argsort(-p)[: max(1, len(p) // 10)]
print(f"purchase intent after 3 views: ROC-AUC {roc_auc_score(yb, p):.3f}, "
      f"PR-AUC {average_precision_score(yb, p):.3f} against a base rate of {yb.mean():.3f}; "
      f"the top 10% convert at {yb[top].mean():.1%} vs {yb.mean():.1%}")
