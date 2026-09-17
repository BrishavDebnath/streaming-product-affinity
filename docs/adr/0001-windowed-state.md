# ADR 0001 — Every stateful operation is windowed

**Status:** accepted

## Context
The first version aggregated with `groupBy("user_id", "product_id")` on a
watermarked stream. That looks correct and passes any short demo.

## Problem
A watermark can only evict state when the event-time column is part of the
grouping key. With business keys alone, Spark retains every key it has ever
seen. Measured on a rate source at 200 rows/s:

| | state rows |
|---|---|
| no window | 0 → 1600 → 2400 → **3200** in 12 s, growing linearly |
| windowed | **flat at 100** across 45 s |

A slow memory leak that is invisible in a five-minute demo and takes the job
down in production.

## Decision
`window(event_time, ...)` is part of the grouping key of every aggregation,
and every stream-stream join carries a time constraint.

## Consequences
State is bounded by watermark + window length. Results are emitted per window
rather than as a running total, so the API aggregates across windows at read
time. `tests/test_transforms.py` asserts `numRowsTotal` stays bounded.
