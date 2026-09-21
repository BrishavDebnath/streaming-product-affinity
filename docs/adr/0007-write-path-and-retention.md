# ADR 0007: Sinks write from executors, and aggregates expire

**Status:** accepted

## Context
The Mongo sinks ran inside `foreachBatch` as:

```python
rows = [r.asDict() for r in batch_df.collect()]
```

## Problem
`collect()` moves the entire micro-batch into the **driver** heap before a
single document is written. On a 12-product catalogue that is 12 rows and
invisible. At realistic cardinality the driver heap becomes the hard ceiling
on batch size, and the job dies on a traffic spike. It is the same class of
failure as ADR 0001, in the write path instead of the state store.

A second problem: nothing ever deleted aggregate rows. The pipeline writes one
document per window per product, every window, forever.

## Decision
- Both sinks use `foreachPartition`, opening one Mongo connection per
  partition and flushing in batches of 1,000. Data stays distributed, and
  writes happen in parallel on the executors.
- MongoDB TTL indexes on `_updated_at` expire `trending`, `product_pairs`
  and `dead_letter` after `RETENTION_HOURS` (default 48).

## Consequences
Write throughput scales with partition count instead of driver memory.
Storage is bounded without a cleanup job. `test_sinks_never_pull_a_batch_into_the_driver`
fails the build if `collect()` or `toPandas()` reappears in a sink.

## Rejected
The MongoDB Spark connector. It would push writes to the executors too, but it
does not express an upsert on a compound natural key as directly, and it added
~15 MB of JARs and minutes of image build time. It has been removed from
`Dockerfile.spark`.
