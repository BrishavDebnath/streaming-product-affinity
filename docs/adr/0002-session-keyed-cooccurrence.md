# ADR 0002: Co-occurrence joins on session, not on user

**Status:** accepted (supersedes the original user-keyed join)

## Context
"Users who viewed A also viewed B" was implemented as a stream-stream self
join on `user_id` within a 10-minute gap.

## Problem
That assumes a user is idle between visits. With 50 users at 20 events/s
every user is continuously active, so "same 10-minute window" stops meaning
"same shopping session":

```
20 events/s ÷ 50 users × 10 min = 240 events per user per window
240 events → 240×239/2          = 28,680 pairs per user
× 50 users                      = 1,314,500 pairs      (~73x too many)
```

Observed in production: 638,467 co-occurrences for a single pair. The number
was wrong, but the ranking was worse: it became meaningless. When everything
pairs with everything, pair count just measures popularity. For a MacBook Air
the top "related products" were the two most-trending phones.

## Decision
The producer stamps a `session_id` per browsing session, and the join keys on
it. Events with no session id (dashboard clicks) fall back to a user-derived
key so they are never silently dropped.

## Consequences
Pair counts fall roughly 73x to a meaningful scale, and related products
outrank bestsellers. The time constraint stays, because it is what bounds the
join state, independent of the session key.
