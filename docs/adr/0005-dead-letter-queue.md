# ADR 0005 — Malformed events are captured, never dropped

**Status:** accepted

## Context
`from_json(...).select("data.*")` turns an unparseable payload into a row of
nulls that flows downstream unnoticed.

## Decision
Validation flags each event with an `invalid_reason`; invalid rows go to a
`dead_letter` collection with the original payload attached. The producer
emits malformed events at `MALFORMED_RATE` so the path is always exercised.

## Consequences
Bad data is quantifiable (`/metrics` exposes the DLQ count) and replayable,
because the raw payload is retained. A rising DLQ rate is a signal that an
upstream producer changed, which silent nulls would hide.
