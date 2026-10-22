# Ten bugs worth knowing about

Most of what went wrong while building this was ordinary. These ten were not: each one
either produced a wrong answer without any error, or only showed up under real load or
real data. Every fix has a test that fails on the old code.


## 1. Streaming state grew without limit

`user_activity_df` grouped by `("user_id", "product_id")` with **no window**,
on a watermarked stream in `update` mode. The watermark cannot evict that
state, because `event_time` is not in the grouping key.

Measured on a rate source at 200 rows/s:

```
BATCH 0  [NO window]  state rows =    0
BATCH 1  [NO window]  state rows = 1600
BATCH 2  [NO window]  state rows = 2400
BATCH 3  [NO window]  state rows = 3200     <- linear growth, no eviction
BATCH 0  [windowed]   state rows =  100
...
BATCH 8  [windowed]   state rows =  100     <- flat across 45 s
```

**Fixed:** every stateful operation is windowed, and a test asserts
`numRowsTotal` stays bounded.


## 2. Every micro-batch duplicated its rows in MongoDB

`outputMode("update")` + `foreachBatch` + `.mode("append")` inserted a *new*
document each micro-batch for a window that already existed, so `/trending`
returned the same product repeatedly with stale counts.

**Fixed:** `bulk_write` with `UpdateOne(..., upsert=True)` on the natural key,
plus unique indexes enforcing it at the database level.


## 3. Mirroring pairs inside the streaming plan silently lost half of them

`pairs.unionByName(flipped)` where `flipped` projects the same streaming
aggregation creates two references to one stateful operator, and only one
branch reliably emits. The streaming query produced `(9001, 9003)` but never
its mirror.

**Fixed:** `mirror_pairs` is applied to the batch DataFrame inside
`foreachBatch`, where the union behaves normally.


## 4. Spark rejected a window on a derived time column

Bucketing on `least(l_time, r_time)` loses event-time attribution and Spark
rejects the aggregation with `STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION`.
Re-declaring the watermark does not work either: Spark 4 raises
*"Redefining watermark is disallowed"*.

**Fixed:** the window buckets on `l_time`, the join's left event time, which
still carries the watermark.


## 5. The Spark job restarted every 20 seconds

A per-process MongoDB client cache, added to stop opening a new client for
every partition, first lived in `job.py`. `spark-submit` runs that file as
`__main__`, and cloudpickle copies whatever a `__main__` function refers to by
value when it sends a partition writer to the Python workers, and that included the
cache. Once the driver had opened a client (which holds thread locks), every
batch failed with `cannot pickle '_thread.lock' object`, the query stopped,
and Docker restarted the container. Checkpoints let it make progress between
crashes, so the dashboard still filled, but `/metrics` showed an input rate of
0 (every progress report was a first batch) and the Spark UI showed each query
running for seconds. Found in the Spark UI's failed-query list.
**Changed:** the cache moved to `src/common/mongo.py`. Functions from an
importable module are sent by reference, so each worker keeps its own cache.
Verified: a Spark run with `job.py` executed as `__main__` and a client
already open in the driver failed with the pickling error before the fix and
reached the worker after it. A new test fails on the old code and passes on
the new.


## 6. Kafka lag always read 0, twice

The listener recorded Spark's `maxOffsetsBehindLatest`, which compares a
batch's end offsets with the latest offsets Spark saw when it planned that
batch. Without `maxOffsetsPerTrigger` a batch reads everything it saw, so
the figure is 0 by construction: it read exactly 0.0 for hours, the Grafana
panel was flat, and the `KafkaLagGrowing` alert could never fire.
**Changed:** after each batch the job asks the broker for its latest offsets
(`kafka_io.latest_offsets`) and subtracts what the batch read. Spark's figure
is only a fallback. The alert now looks for a rising floor, because the lag
is a sawtooth and its slope is noise. Each batch also records how many rows
it read.

The first fix never ran, and that only showed up on the live stack: lag still
read exactly 0.0. The lag reader passed
`api_version_auto_timeout_ms`, which kafka-python 3 renamed to
`bootstrap_timeout_ms`, so building the client failed on every batch and the
listener quietly fell back to Spark's always-0 figure. **Changed:** the
settings live in `kafka_io.LAG_CONSUMER_CONFIG`, and a test checks every key
against `KafkaConsumer.DEFAULT_CONFIG`. Without the broker, lag is now
recorded as unknown rather than 0 (Spark's figure is used only when
`MAX_OFFSETS_PER_TRIGGER` caps batches), and a failed attempt is not retried
for 30 s, because each one blocks for about 5 s.


## 7. A replay silently lost its last two minutes

Found on the first real run. Spark advances a watermark on event time, and
during a replay the replay is the only thing producing event time. When it
stopped, the windows holding its final minutes never closed. On a
five-minute slice the pair counts quietly missed most of the last two minutes,
and nothing in the logs said so. **Changed:** the replay now ends by sending a
few events timestamped past the end of the slice, which pushes the watermark
over the line. They use a reserved product id (-1) and a fresh session each,
so they can form no pair, and their own trending rows are deleted once Spark
has written them. `--flush-only` does it for a replay that has already run.
Two tests assert the events move time forward and change nothing else.


## 8. The evaluation could score the pipeline on its own training days

The worst bug in the project, because it produced a *better* number
rather than an error. `scripts/evaluate.py --train-days 7 --test-days 7` split
the dataset at day 7 and tested on days 7 to 14, while the running pipeline held
a 30-day replay. Days 7 to 14 were therefore in the test set *and* in the
pipeline's pair table. It reported **35.100% hit-rate@10, 81x the baseline,
100% coverage**. Measured honestly on unseen days the same pipeline scores
**17.433%**. A doubled score, from a command that looked entirely reasonable.

Nothing in the data could reveal it. The pipeline's rows carry replay-clock
timestamps (the wall clock of the machine at replay time), not dataset dates,
so no query on `product_pairs` can tell you which days of 2015 produced them.
The split lives in `evaluate.py` and the load lives in `replay.py`. Neither
knew what the other had done.

**Changed:** the replay now writes a manifest. After a successful send,
`record_run()` inserts `{dataset, start_day, days, events, speedup,
finished_at}` into `replay_runs`, and the evaluation reads the latest one
before it builds a single test case. If the training window it was asked for
is not the window that was replayed, it names both and exits 2:

```
the pipeline holds a 30-day replay from 2015-05-03, but this asks for
7 days from 2015-05-03. Testing on days the pipeline was trained on
inflates the score; replay that window first, or pass
--allow-window-mismatch to measure anyway.
```

`--allow-window-mismatch` still measures, loudly, for the case where you do
know better. A missing manifest is not an error. Older stacks and hand-driven
pipelines have none, and refusing to measure at all would be worse than
measuring unguarded.

Five tests: a matching window passes, a mismatched window and a mismatched
start day are both caught, the replay's record and the evaluation's reader
agree on shape, and end to end a leaking run exits 2 and writes no report
while the override writes one.

**The 35.1% figure is withdrawn as a result.** Both the README and
`docs/EVALUATION.md` mention it only as the example of what this mistake
looks like. The published numbers are the 30-day ones in those two files.


## 9. The baseline was too easy to beat

The only baseline was the ten most-viewed products overall. While testing ML
ideas for the roadmap, a stronger and just as simple one turned up: the ten
most-viewed products in the query's own category. On the same 3,000 test
cases it scored 19.7% hit-rate@10 at full coverage, the same as the
pipeline's published co-occurrence figure. The "26.9x" claim was true, but
measured against the wrong thing, and anyone trying the obvious comparison
would have found that.

**Changed:** `scripts/evaluate.py` now scores the category baseline on the
same cases and reports the pipeline's difference from it in points, next to
the old ratio. The API fills the slots co-occurrence leaves empty with the
query category's most active products before falling back to trending
(offline, that fill took the same test cases from 15.2% to 26.8%). Every
related product now says where it came from, in the API and on the
dashboard. `scripts/replay.py` also stopped overwriting a catalogue that
already covers every replayed item, because the category baseline needs
categories for every item, not only the replayed month's. Five tests.


## 10. The API cache never evicted anything

`_cached` wrote entries and never removed them. The category index is keyed by
the lookback minute, so it added an entry every minute and kept it, each one
holding a row per catalogued item; query parameters also went straight into
keys. Harmless on twelve demo products, hundreds of megabytes on a real
catalogue.
**Changed:** entries past the longest TTL are dropped on write, and the cache
is capped at `CACHE_MAX_KEYS` (256) with the oldest going first. `/metrics`
reports how many entries are held. Two tests.
