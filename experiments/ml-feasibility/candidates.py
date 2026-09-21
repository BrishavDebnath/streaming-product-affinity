"""Which extra candidate sources would raise the ranker's ceiling."""
import collections

import numpy as np

from common import Features, cases_for, catalog, parent, window

F = Features(window(0, 30))
cases = cases_for(30, 37)
loose = collections.defaultdict(list)
for (x, y), n in F.pc.items():
    loose[x].append((n, y))
    loose[y].append((n, x))
for x in loose:
    loose[x].sort(reverse=True)
by_parent = collections.defaultdict(collections.Counter)
for item, n in F.views.items():
    by_parent[parent.get(catalog.get(item))][item] = n
parent_top = {p: [i for i, _ in c.most_common(30)] for p, c in by_parent.items()}


def two_hop(q, n=30):
    score = collections.Counter()
    for w1, mid in F.nbr.get(q, [])[:10]:
        for w2, z in F.nbr.get(mid, [])[:10]:
            if z != q:
                score[z] += w1 * w2
    return [z for z, _ in score.most_common(n)]


seen_by = collections.defaultdict(set)
for visit in F.visits:
    seen_by[visit.visitor].update(visit.items)
cross = collections.Counter()
for items in seen_by.values():
    if 2 <= len(items) <= 30:
        ordered = sorted(items)
        for i, a in enumerate(ordered):
            for b in ordered[i + 1:]:
                cross[(a, b)] += 1
across = collections.defaultdict(list)
for (a, b), n in cross.items():
    if n >= 2:
        across[a].append((n, b))
        across[b].append((n, a))
for x in across:
    across[x].sort(reverse=True)

sources = [
    ("current candidates (co-occurrence, category, item2vec, global)", lambda q: F.candidates(q)[0]),
    ("plus co-occurrence with no minimum count", lambda q: [y for _, y in loose.get(q, [])[:30]]),
    ("plus parent-category bestsellers", lambda q: parent_top.get(parent.get(catalog.get(q)), [])),
    ("plus two-hop co-occurrence", two_hop),
    ("plus the same visitor's other visits", lambda q: [y for _, y in across.get(q, [])[:30]]),
]
pool = [set() for _ in cases]
for name, source in sources:
    for i, case in enumerate(cases):
        pool[i].update(source(case.query))
    recall = sum(c.target in pool[i] for i, c in enumerate(cases)) / len(cases)
    print(f"{name:<66} recall {recall:.1%} ({np.mean([len(s) for s in pool]):.0f} candidates)")
