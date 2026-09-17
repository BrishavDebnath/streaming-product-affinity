# ADR 0006 — The consumer reads every schema version in flight

**Status:** accepted

## Context
Events gained two fields after the first release: `session_id` (needed for
ADR 0002) and `channel`.

## Problem
A producer fleet is never upgraded atomically. During a rollout, v1 and v2
events are on the topic at the same time. A consumer that assumes the newest
schema drops or crashes on the older ones, and the usual workaround — stop
producers, deploy, restart — is downtime.

## Decision
`EVENT_SCHEMA` is the **union** of all live versions, with fields added after
v1 declared nullable. `parse_events` defaults them after validation, so a
missing optional field is never a rejection reason:

| version | carries | handling |
|---|---|---|
| v1 | no `schema_version`, no `session_id`, no `channel` | `schema_version` defaults to 1, `channel` to `"unknown"`, co-occurrence falls back to a user-derived session key |
| v2 | all fields | used directly |
| v3+ | unknown to this consumer | routed to the DLQ as `unsupported_schema_version:N` |

The producer emits a `LEGACY_EVENT_RATE` share of v1 events continuously, so
the compatibility path is always exercised rather than being dead code that
rots.

## Consequences
Producers and consumers deploy independently. A consumer running ahead of its
producers is safe; a producer running ahead of its consumers is **detected**
rather than silently mis-parsed — the row lands in the DLQ with its original
payload, so it can be replayed once consumers catch up.

## Rejected
Confluent Schema Registry with Avro. It is the right answer at scale, but it
adds a container, a registry to keep alive, and a code-generation step — a
lot of configuration to demonstrate a principle that 50 lines shows directly.
