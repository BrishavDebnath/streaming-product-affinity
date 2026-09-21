# ADR 0003: Rank by lift, not by raw co-occurrence count

**Status:** accepted

## Context
Even with session-scoped pairing, a bestseller appears in many sessions and
so co-occurs with everything.

## Decision
`/related-products/{id}?score_by=lift` divides popularity out:

```
lift(A,B) = P(A,B) / (P(A) · P(B))
```

Marginals come from the trending collection over the same lookback window.
`affinity` (weighted count) remains the default so both are inspectable.

## Consequences
A laptop sleeve is rarely viewed alone and almost always viewed with a laptop.
It has a low count and a high lift, and now ranks where it should. Lift is
undefined for a product with no interactions yet, so the API returns `null`
and flags `score_undefined` instead of silently substituting zero.

## Rejected
Normalising inside Spark. Lift needs per-product marginals, which is a second
streaming aggregation and another stateful operator. Computing it at read
time over already-aggregated data is simpler and equally correct.
