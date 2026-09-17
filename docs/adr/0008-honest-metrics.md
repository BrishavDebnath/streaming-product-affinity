# ADR 0008 — A metric is named for what it measures

**Status:** accepted

## Context
Spark computes `approx_count_distinct(user_id)` per window. The API summed
across a 30-minute range and returned the `$max` of those values as
`unique_users`.

## Problem
The maximum of thirty approximate per-minute distinct counts is not the
distinct count over thirty minutes. If 40 different users touch a product in
each of 30 windows, the true figure is anywhere between 40 and 1,200 — and the
dashboard said 40. Not an approximation: a different quantity entirely, shown
under a name that implied otherwise.

HyperLogLog sketches merge correctly across windows, which is how this would
be computed properly. Spark's `approx_count_distinct` returns a number, not
the sketch, so the sketches are gone by the time the API sees the data.

## Decision
The field is `peak_users_per_window` — which is exactly what `$max` of
per-window counts gives. An exact cross-window figure would require persisting
HLL sketches and merging them at read time.

## Consequences
The dashboard number is correct for its label. `test_no_misleading_unique_user_counts`
fails the build if `unique_users` is emitted for a multi-window range again.

A wrong number under a plausible label is worse than no number: nobody
double-checks a plausible one.
