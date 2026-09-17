# ADR 0004 — Sinks upsert on a natural key

**Status:** accepted

## Context
The original sink used `outputMode("update")` with `.mode("append")`.

## Problem
Every micro-batch inserted a NEW document for a window that already existed,
so `/trending` returned the same product repeatedly with stale counts, and a
replay after failure doubled everything.

## Decision
`bulk_write` with `UpdateOne(key, {"$set": row}, upsert=True)` on
`(window_start, window_end, product_id)` — and the equivalent for pairs —
backed by unique indexes so the database enforces it too.

## Consequences
Writes are idempotent: reprocessing the same batch converges rather than
duplicating. This is what makes checkpoint-based restart safe without
distributed transactions. Verified by
`test_replaying_a_batch_does_not_double_count`.
