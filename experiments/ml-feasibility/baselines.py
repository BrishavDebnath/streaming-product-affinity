"""Every simple method on the pipeline's split: 30 days to train, 7 to test."""
import collections
import random

from gensim.models import Word2Vec

from common import (
    SEED,
    K,
    cases_for,
    catalog,
    co_view_pairs,
    ev,
    neighbours,
    visits_in,
    window,
)

train_ev = window(0, 30)
train_visits = visits_in(0, 30)
cases = cases_for(30, 37)
sample = list(cases)
random.Random(SEED).shuffle(sample)
sample = sample[:3000]
views = collections.Counter(e.item for e in train_ev if e.event_type == "view")
print(f"train events {len(train_ev):,}, test cases {len(cases):,} (and a 3,000 sample)")


def score(name, recommend):
    a = ev.evaluate(name, cases, recommend, K)
    b = ev.evaluate(name, sample, recommend, K)
    print(f"{name:<48} all: hit@10 {a.hit_rate:6.2%} cov {a.coverage:6.1%} | "
          f"3k: hit@10 {b.hit_rate:6.2%} cov {b.coverage:6.1%}")


def listing(nbr):
    return lambda q: [y for _, y in nbr.get(q, [])][:K + 1]


def blend(*sources):
    def recommend(q):
        out = []
        for source in sources:
            for x in source(q):
                if x != q and x not in out:
                    out.append(x)
        return out[:K + 1]
    return recommend


top = ev.bestsellers(views, K)
score("global bestsellers (the published baseline)", lambda q: top)
nbr = neighbours(co_view_pairs(train_visits))
score("co-occurrence (the pipeline's rule)", listing(nbr))

per_visitor = collections.Counter(e.visitor for e in train_ev)
for threshold in (200, 100, 50):
    heavy = {v for v, n in per_visitor.items() if n >= threshold}
    kept = [v for v in train_visits if v.visitor not in heavy]
    score(f"co-occurrence without visitors of {threshold}+ events",
          listing(neighbours(co_view_pairs(kept))))

by_cat = collections.defaultdict(collections.Counter)
for item, n in views.items():
    by_cat[catalog.get(item)][item] = n
cat_top = {c: [i for i, _ in cnt.most_common(K + 1)] for c, cnt in by_cat.items()}
category = lambda q: cat_top.get(catalog.get(q), [])  # noqa: E731
score("category bestsellers", lambda q: category(q) or top)

w2v = Word2Vec([[str(i) for i in v.items] for v in train_visits if len(v.items) >= 2],
               vector_size=64, window=5, min_count=2, sg=1, negative=10, epochs=15,
               workers=2, seed=SEED, sample=1e-4)
item2vec = lambda q: ([int(x) for x, _ in w2v.wv.most_similar(str(q), topn=K + 1)]  # noqa: E731
                      if str(q) in w2v.wv else [])
score("item2vec", item2vec)
score("co-occurrence, then item2vec", blend(listing(nbr), item2vec))
score("co-occurrence, then category bestsellers", blend(listing(nbr), category))
known = set(views)
print(f"test queries seen in training {sum(c.query in known for c in cases) / len(cases):.1%}, "
      f"targets seen in training {sum(c.target in known for c in cases) / len(cases):.1%}")
