"""What the data allows: visit lengths, heavy visitors, conversion rates."""
import collections

from common import events, rr

visits = list(rr.sessionise(rr.sort_events(events)))
sizes = [len(v.items) for v in visits]
print(f"events {len(events):,}  visits {len(visits):,}")
for n in (1, 2, 3, 5):
    print(f"visits with {'exactly' if n == 1 else 'at least'} {n} distinct items: "
          f"{sum((s == 1) if n == 1 else (s >= n) for s in sizes) / len(visits):.1%}")
per_visitor = collections.Counter(e.visitor for e in events)
visits_per = collections.Counter(v.visitor for v in visits)
print(f"visitors {len(per_visitor):,}, returning (2+ visits) "
      f"{sum(n >= 2 for n in visits_per.values()) / len(visits_per):.1%}")
heavy = [v for v, n in per_visitor.items() if n >= 200]
print(f"visitors with 200+ events: {len(heavy)}, "
      f"holding {sum(per_visitor[v] for v in heavy) / len(events):.1%} of events; "
      f"top ten: {[n for _, n in per_visitor.most_common(10)]}")
cart = sum(any(e.event_type == "add_to_cart" for e in v.events) for v in visits)
buy = sum(any(e.event_type == "purchase" for e in v.events) for v in visits)
print(f"visits with an add-to-cart {cart / len(visits):.2%}, with a purchase {buy / len(visits):.2%}")
