# Defects found in the original project, and what was done

> **Naming.** The project was renamed from *Real-Time E-Commerce
> Recommendation System* to **Streaming Product Affinity Pipeline** (see the
> Eighth pass). Entries written before the rename keep the old names -
> `/recommendations`, the `recommendations` collection, `recsys_` metrics -
> because they describe the code as it was then.

Audited against the original `e-commerce-recommendation` archive (248 lines).
Claims marked **verified** were tested with a real Spark session, not inferred
from reading the code.

---

## Blocking

### 1. There was no recommendation algorithm
`consumer.py` computed two aggregations: event counts per product per window,
and interaction counts per `(user_id, product_id)`. Neither is a
recommendation. The project's own `notes.txt` specified *"User who viewed X
product also viewed Y product"* — that logic did not exist.

**Fixed:** a stream-stream self-join (first on `user_id`, later on the
browsing session - see ADR 0002) with a time constraint
produces genuine co-occurrence pairs, weighted by event type. See
`transforms.co_occurrence`.

### 2. Unbounded state — **verified**
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

### 3. Duplicate documents in MongoDB
`outputMode("update")` + `foreachBatch` + `.mode("append")` inserted a *new*
document each micro-batch for a window that already existed, so `/trending`
returned the same product repeatedly with stale counts.

**Fixed:** `bulk_write` with `UpdateOne(..., upsert=True)` on the natural key,
plus unique indexes enforcing it at the database level.

### 4. Producer and dashboard used different product spaces
`producer.py` emitted product IDs 9001–9005; `ui/dashboard.py` emitted
101–105, which were the producer's **user** IDs. The dashboard matched
`product['id'] == product_id`, so an event from the producer could never
render. Users also differed: producer 101–105, dashboard hardcoded 1001.

**Fixed:** `src/common/catalog.py` is the single catalogue, imported by
producer, API and UI. A test asserts they share one product space.

---

## Correctness

### 5. `/recommended/{product_id}` returned trending, not recommendations
It queried `collection`, which was `db['trending']`. The endpoint name did not
match its behaviour.

### 6. Two functions both named `get_trending`
`services/app.py` lines 19 and 28. The second shadowed the first.

### 7. An unimplemented TODO shipped in source
`# Task-1: Sort products in descending order based on event_count` — the
endpoint returned `list(collection.find({}))`, unsorted and unlimited, mixing
every window ever written.

**Fixed:** results are summed over `TRENDING_LOOKBACK_MINUTES` of windows, sorted by weighted score
descending, and limited.

### 8. `notes.txt` advertised a Gemini explanation feature
No Gemini code existed anywhere in the archive.

**Fixed:** removed. Nothing in the documentation now describes a feature that
is not implemented.

### 9. Malformed events became silent nulls
`from_json(...).select("data.*")` turns an unparseable payload into a row of
nulls that flows downstream unnoticed.

**Fixed:** validation flags each event, invalid ones route to a dead-letter
collection with the reason and the original payload. The producer emits
malformed events at a configurable rate so the path stays exercised.

### 10. Producer rate made trending meaningless
`time.sleep(30)` against a 1-minute window gave roughly two events per window
across five products.

**Fixed:** configurable `EVENTS_PER_SECOND`, default 20, emitted as sessions.

---

## Corrected from my own earlier review

I previously reported that `to_timestamp(col("timestamp"))` on a `DoubleType`
column was **likely broken and probably yielded null**. That was wrong.
Tested directly:

```
raw timestamp value: 1787636485.393883
to_timestamp(double)  -> 2026-08-25 05:41:25.393883
cast('timestamp')     -> 2026-08-25 05:41:25.393883
```

It works. The rewrite uses `.cast("timestamp")` anyway, because the cast is
unambiguous for a numeric column across Spark versions, but the original code
was **not** defective here.

---

## Discovered while rebuilding

### 11. Mirroring pairs inside the streaming plan does not work
`pairs.unionByName(flipped)` where `flipped` projects the same streaming
aggregation creates two references to one stateful operator, and only one
branch reliably emits — the streaming query produced `(9001, 9003)` but never
its mirror.

**Fixed:** `mirror_pairs` is applied to the batch DataFrame inside
`foreachBatch`, where the union behaves normally.

### 12. A windowed aggregation on a derived time column is rejected
Bucketing on `least(l_time, r_time)` loses event-time attribution and Spark
rejects the aggregation with `STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION`.
Re-declaring the watermark is not a workaround either — Spark 4 raises
*"Redefining watermark is disallowed"*.

**Fixed:** the window buckets on `l_time`, the join's left event time, which
still carries the watermark.

---

## Hygiene

| | |
|---|---|
| `db/mongo_config.py` was 0 bytes | removed; config lives in `src/common/config.py` |
| No `requirements.txt` | version ranges, verified (see #14) |
| No `README`, no `.gitignore` | both added |
| `__pycache__/` committed | git-ignored |
| Hostname/port hardcoded in five files | all environment-driven |
| Service named `mongo`, container `mongodb`, code used `mongodb` | one name throughout |
| ZooKeeper container | Kafka in KRaft mode; ZooKeeper is removed in Kafka 4.x |
| No health checks — Spark raced the broker | `condition: service_healthy` |
| No tests | 74 tests (Spark, API, dashboard), plus CI on every push |


---

# Fourth pass — dependency and version portability

Found by installing the pinned requirements on a clean Python 3.12, rather
than by reading them.

### 13. `kafka-python==2.0.2` does not work on Python 3.12
Fails at import:

```
ModuleNotFoundError: No module named 'kafka.vendor.six.moves'
```

The producer, the dashboard and the smoke test all import it, so nothing that
touches Kafka would have started. **Fixed:** `kafka-python>=2.2`; verified
working on 3.0.11.

### 14. Pinning PySpark to the image version was wrong and fragile
`pyspark==3.5.3` was pinned to match `apache/spark:3.5.3`. That is
unnecessary — the host's PySpark is used only by the test suite; the job runs
inside the container with the image's own PySpark. **Fixed:** relaxed to
`pyspark>=3.5,<5.0`, and the suite is verified on **both 3.5.3 and 4.2.0**.

### 15. The streaming test only passed on Spark 4
On **Spark 3.5**, a query with chained stateful operators (stream-stream join
followed by a windowed aggregation) uses a *global* watermark that lags one
micro-batch. Two input batches emit nothing; the window never closes. Spark 4
emits on two.

Diagnosed by instrumenting the watermark:

```
t=  5s rows=0 watermark=None
t= 10s rows=0 watermark=1970-01-01T00:00:00.000Z
t= 15s rows=2 watermark=2023-11-14T22:43:21.000Z
```

**Fixed:** the test feeds three batches, which works on both. This matters
beyond the test — it is why the pipeline needs a continuously running producer
to emit recommendations at all. A burst of events followed by silence leaves
the last window open.

### 16. The streaming test was non-deterministic
It broke out of its polling loop on the *first* rows to appear, which on 3.5
was a later batch's pair. It also assumed the file source reads files in
filename order; it orders by **modification time**. **Fixed:** files are
written with a pause between them, and the test polls until the specific
expected pair appears.

### 17. `_updated_at` was never actually set
`row["_updated_at"] = row.get("_updated_at")` set the field to `None` — the
key never existed. Dead code. **Fixed:** records the real UTC write time,
which is what makes the `/pipeline` processing-lag metric possible.


---

# Fifth pass - pre-release test

Found by running the suite on PySpark 3.5 (the container's version), driving
the monitoring listener with a live streaming query, and calling every API
endpoint against a test database.

### 18. The dashboard crashed as soon as trending had data
ADR 0008 renamed the API field `unique_users` to `peak_users_per_window`, but
`dashboard.py` still read `r["unique_users"]`: a `KeyError` in the Trending
panel. **Fixed**, and a test now checks the dashboard against the API's field.

### 19. The monitoring listener never ran
`class _Listener(StreamingQueryListener, ProgressRecorder)` put the abstract
base first, so its abstract callbacks won the method lookup and Python refused
to instantiate the class. The job caught the `TypeError` and carried on, so
four of the six Grafana panels (Kafka lag, ingest rate, batch duration, state
rows) stayed empty. Underneath it, `onQueryProgress` called `.get()` on
PySpark's progress *objects*, which raised `AttributeError` on every batch.
**Fixed:** base order swapped, attributes read with `getattr`, Kafka lag parsed
to a number. **Verified** on a live streaming query; covered by a test.

### 20. `.env` was never read
Nothing loaded it, so `cp .env.example .env` and the RUNBOOK's "edit `.env`"
advice changed nothing. **Fixed:** `config.py` loads it through
`python-dotenv`, without overriding variables that are already set.

### 21. Dead-letter rows never expired
The TTL index is on `_updated_at`, which only the upsert sink set. **Fixed:**
the dead-letter sink sets it too.

### 22. `/graph?limit=` returned up to four times the limit
A copied over-fetch line. **Fixed.**

### 23. An undefined lift could outrank a real one
With `score_by=lift`, a pair with no lift fell back to its affinity (e.g. 2.0)
and outranked real lift values (e.g. 0.42) - two different scales compared as
one. **Fixed:** defined scores rank first; covered by a test.

### 24. The smoke test could not fail its DLQ check, and could time out
The check was `count >= 0`. It now sends a malformed event and requires the
dead-letter count to rise. The wait was 240 s, shorter than the worst case for
a 5-minute window plus the 2-minute watermark; it is now 540 s, with a
heartbeat event each poll so windows close even without the producer.

### 25. The README did not let a stranger run the project
No prerequisites, no clone step, no Windows commands (the Makefile does not
run in PowerShell), links to screenshots and a benchmarks file that did not
exist, two different wrong test counts, and a join snippet still keyed on
`user_id`. **Fixed.**


---

# Sixth pass - first full run on Windows

Found by running the whole stack on Windows 11 with Docker Desktop.

### 26. Recommendations took ~17 minutes to appear
With `COOCCURRENCE_WINDOW=5 minutes` and `CO_VIEW_GAP=10 minutes`, the
dashboard showed zero pairs long after trending was full. Spark 3.5 holds a
time-interval join's output back by the join's time range, so a pair is saved
only when events arrive about window + gap + watermark later. **Measured** with
a file-source stream: 5/10 minutes emitted at +17 minutes of event time; 1/2
minutes at +5. **Fixed:** defaults are now 1 minute and 2 minutes; a session
never spans more than ~100 s, so no pairs are lost. A test pins the defaults.

### 27. Seed events could be skipped entirely
The job read Kafka from `latest`, so events seeded while Spark was still
starting (exactly what `make demo` does) were never read. **Fixed:** a new
query starts from `earliest` (`STARTING_OFFSETS`); restarts still resume from
the checkpoint. Seeding after the producer has started drops back-dated events
as late - now documented in the README, RUNBOOK and `seed.py`.

### 28. "Processing lag" showed negative numbers
Trending runs in update mode, so the newest row is rewritten while its window
is still open, and `written - window_end` came out as -29.5 s. **Fixed:** the
delay is measured on the newest window whose last write came after it ended.

### 29. Dashboard wording
Internal names leaked into the page: "Documents" (MongoDB's term for rows),
"Data age", "Acting as user", "Affinity", "Co-occurrences", "Cold start",
category ids like `laptop-acc`. **Fixed:** plain labels throughout ("Saved in
MongoDB: Trending rows, Product pairs, Rejected events", "Match score",
"Times seen together", "Laptop accessories"), and the fallback table no
longer shows empty affinity columns.

### 30. Every producer printed a DeprecationWarning
kafka-python 3.x deprecates lambda serializers; PowerShell renders each
warning as a red error. **Fixed:** `src/common/kafka_io.py` provides real
`Serializer` classes, used by the producer, dashboard and all scripts.

### 31. Scripts looked frozen when their output was redirected
Python buffers stdout when it is piped, so `smoke_test.py | Out-File` showed
nothing for up to nine minutes. **Fixed:** the scripts line-buffer their
output.

### 32. `use_container_width` is deprecated in Streamlit
**Fixed:** `width="stretch"`; `streamlit>=1.50` (verified on 1.50 and 1.64).

### 33. Grafana opened on its own setup page, as an anonymous Admin
Visitors landed on "Welcome to Grafana" and had to find the dashboard
themselves, and the compose file gave anonymous visitors the Admin role while
its comment said "viewer". **Fixed:** the pipeline dashboard is the home page
and anonymous access is Viewer. The README explains the "Unauthorized" pop-up
(a stale cookie from an earlier Grafana) and that the API must listen on
`0.0.0.0` for Prometheus to reach it.

### 34. The Spark image stopped building
Item 32 raised Streamlit to 1.50, but the Spark image installed the host's
`requirements.txt` on its own Python 3.8, where Streamlit ends at 1.40.1, so
`docker compose build` failed. **Fixed:** the image installs
`requirements-spark.txt` - only pymongo, python-dotenv and kafka-python, which
is all the job and the tests import. Resolution checked for Python 3.8.

---

# Seventh pass - checking the live stack

Found by reading the running API, Prometheus and Grafana directly.

### 35. "Events in the latest minute" showed half the real rate
It read the minute still in progress (578 events, 9.6/s against ~1,160 and
19.4/s for full minutes). **Fixed:** the dashboard uses the last minute that
has ended.

### 36. `/metrics` mixed three Spark queries into one reading
It reported whichever query wrote progress last, so the input rate jumped
between ~19/s (trending) and ~39/s (recommendations, whose self-join reads the
stream twice). **Fixed:** every Spark metric carries a `query` label, and the
Grafana panels show one line per query.

### 37. "Ingest vs processing rate" charted only the ingest rate
**Fixed:** it plots read and processed rates for the trending query.

### 38. Wrong explanation of Grafana's "Unauthorized" pop-up
Item 33 blamed a stale cookie. Checked on the running stack: Grafana's page
calls `/api/user`, which returns 401 for anonymous visitors. **Fixed** in the
README; the pop-up is harmless.

### 39. Chance pairs cluttered recommendations and the graph
Pairs seen twice with lift ~0.02 (MacBook -> iPhone) appeared in both. They
came from seeded v1 events, which had no `session_id` and so paired by shopper
across separate visits. **Fixed:** seeded v1 events carry a session id, and
pairs seen fewer than `MIN_PAIR_COUNT` (3) times are left out.

---

# Eighth pass - rename and platform upgrade

### 40. The name promised something the project does not do
It was called a recommendation system, but nothing is personalised and nothing
is learned: it finds products viewed together in a session. **Changed:** the
project is the **Streaming Product Affinity Pipeline**;
`/recommendations/{id}` is `/related-products/{id}` (response key
`related_products`); the `recommendations` collection is `product_pairs`;
`RECOMMENDATION_LOOKBACK_MINUTES` is `PAIR_LOOKBACK_MINUTES`; the database is
`product_affinity`; metrics use the `affinity_` prefix; the Spark query and
checkpoint are `product_pairs`; containers are prefixed `affinity-`.

### 41. The Spark image ran an end-of-life Python
`apache/spark:3.5.3` ships Python 3.8, unsupported since October 2024.
**Changed:** Spark 4.1.3 (`apache/spark:4.1.3-python3`: Java 17, Scala 2.13,
Python 3.10) with the matching `spark-sql-kafka-0-10_2.13` connector. The Ivy
cache path is set explicitly because Spark 4 moved its default. **Verified:**
the full suite passes on PySpark 4.1.3; the watermark delay (item 26) and the
progress listener (item 19) behave the same as on 3.5.

### 42. Kafka ran a vendor build of 7.7
**Changed:** the official `apache/kafka:4.3.1` image in KRaft mode, with its
own health-check path and log directory, and `kafka-python>=3.0`, whose
protocol tables cover Kafka 4.x. MongoDB moved from 7.0 to 8.0.

### 43. Spark 4.1 flooded the log with a harmless stack trace
Several times per micro-batch: `WARN StreamingJoinHelper: Error trying to
extract state constraint ... Cannot evaluate expression: l_product`. Spark 4.1
also inspects the non-time comparison `l_product < r_product` when working
out how long to keep join state. Reordering the condition does not help (the
optimizer normalises it). **Measured:** over ten and twenty minutes of steady
sessions the join state levels off at ~600 rows and pairs are emitted on
time, so eviction works. **Changed:** only that logger is set to ERROR, and a
new test feeds ten minutes of sessions and fails if the join state keeps
growing.


---

# Ninth pass - realistic data, parallelism, one-command setup

### 44. The demo data never crossed categories
Every session picked all its products from categories related to the first
one (`catalog.AFFINITY`), so a shopper who looked at shoes never looked at a
phone. The graph showed perfectly separate clusters, which real clickstreams
never do, and lift had nothing to separate. **Changed:**
`catalog.session_products` sends each further view to any product with
probability `CROSS_CATEGORY_RATE` (default 0.15). The producer and the seed
both use it. **Measured:** related pairs score lift 6-14; cross-category pairs
0.7-3.5, so the ranking still separates them. A test checks that a rate of 0
never crosses and a rate of 1 does. The dashboard graph draws cross-category
links dashed and has a slider for how many links to show; at the old fixed
limit of 30 the weakest cross links (shoes + phones) were never drawn.

### 45. One Kafka partition
Kafka auto-created `clickstream` with one partition, so Spark read it with one
task. **Changed:** auto-creation is off; a `kafka-init` service creates the
topic with `KAFKA_PARTITIONS` (6) before anything else starts.

### 46. Spark used 200 shuffle partitions and the heap state store
**Changed:** `SHUFFLE_PARTITIONS` (8, matching `local[8]`) and
`STATE_STORE=rocksdb` with changelog checkpointing. `MAX_OFFSETS_PER_TRIGGER`
can cap batch size. A test checks the RocksDB provider class loads. See
ADR 0009.

### 47. Checkpoints lived in a host folder
`.checkpoints` was bind-mounted from the project folder: slow on Windows, left
behind by `docker compose down -v`, and easy to delete while Spark was running.
**Changed:** the `spark_checkpoints` named volume. `down -v` now resets
everything.

### 48. A new MongoDB client for every partition of every batch
Each client opens its own connection pool and monitoring threads.
**Changed:** one cached client per process (`src/common/mongo.py`), shared by
the writers and the progress listener. The first version kept the cache in
`job.py` and crashed the job - see item 52.

### 49. Running the project needed four terminals and a host Python
**Changed:** the API, dashboard, producer, seed and a `smoke` tool service run
in Compose from one image (`Dockerfile.app`, non-root user). Start-up order is
declared with health and completion conditions. `seed --if-empty` skips the
backfill when results already exist, so restarting the stack does not inject
back-dated events that would be dropped as late.

### 50. Nothing alerted when the pipeline stopped
**Changed:** `/metrics` exports `affinity_pipeline_staleness_seconds` and
`affinity_processing_delay_seconds`; `monitoring/alerts.yml` defines
`ApiDown`, `PipelineStale`, `KafkaLagGrowing`, `BatchSlowerThanTrigger` and
`StateStoreGrowing`.

### 51. The first one-command build failed on Docker Desktop
The four Python services shared one `image:` name. Compose builds them in
parallel, and with Docker Desktop's image store each build tried to tag the
same name: `image "streaming-product-affinity-app:latest": already exists`.
**Changed:** no shared name; each service gets its own tag
(`streaming-product-affinity-api`, `-dashboard`, ...). The layers are
identical, so they are built and stored once. A test fails if the shared name
comes back.

### 52. The Spark job restarted every ~20 seconds (introduced by item 48)
The client cache first lived in `job.py`. `spark-submit` runs that file as
`__main__`, and cloudpickle copies whatever a `__main__` function refers to by
value when it sends a partition writer to the Python workers - including the
cache. Once the driver had opened a client (which holds thread locks), every
batch failed with `cannot pickle '_thread.lock' object`, the query stopped,
and Docker restarted the container. Checkpoints let it make progress between
crashes, so the dashboard still filled, but `/metrics` showed an input rate of
0 (every progress report was a first batch) and the Spark UI showed each query
running for seconds. **Found** in the Spark UI's failed-query list.
**Changed:** the cache moved to `src/common/mongo.py`; functions from an
importable module are sent by reference, so each worker keeps its own cache.
**Verified:** a Spark run with `job.py` executed as `__main__` and a client
already open in the driver failed with the pickling error before the fix and
reached the worker after it; a new test fails on the old code and passes on
the new.

### 53. Three quarters of the Spark log was py4j chatter
Measured over 1 h 38 min of the running job: 3,562 of 4,752 lines were
`py4j.clientserver | Received command c on object id ...`, and the unit-test
log opened with ~40 Spark INFO start-up lines. **Changed:** the `py4j` logger
is set to WARNING in `job.py`, and the test session sets `spark.log.level` so
start-up is quiet. The same 1 h 38 min had no Spark errors and three one-off
start-up warnings.

---

# Tenth pass - measurement

### 54. Kafka lag was always 0
The listener recorded Spark's `maxOffsetsBehindLatest`, which compares a
batch's end offsets with the latest offsets Spark saw when it planned that
batch. Without `maxOffsetsPerTrigger` a batch reads everything it saw, so
the figure is 0 by construction: it read exactly 0.0 for hours, the Grafana
panel was flat, and the `KafkaLagGrowing` alert could never fire.
**Changed:** after each batch the job asks the broker for its latest offsets
(`kafka_io.latest_offsets`) and subtracts what the batch read; Spark's figure
is only a fallback. The alert now looks for a rising floor, because the lag
is a sawtooth and its slope is noise. Each batch also records how many rows
it read.

### 55. Write timestamps were taken before the batch was computed
`_updated_at` was set when `foreachBatch` started. Spark computes the batch
lazily after that, so freshness, processing delay and any latency measured
from it read too low by up to a batch duration. **Changed:** rows are stamped
when each chunk is written. A test runs a sink against a batch that takes
0.3 s to "compute" and checks the stamps come after it.

### 56. The load test measured the wrong things
Two-event sessions of fixed products from one process; "lag" taken from the
window-close delay; hardware left for the reader to fill in; no latency.
**Changed:** rewritten (`scripts/load_test.py`, `docker compose run --rm
loadtest`) - realistic sessions from several processes, Spark's per-batch
records, real lag, a stated "kept up" rule, probe latency, machine details
detected, and a report section in `docs/BENCHMARKS.md`. See ADR 0010.

### 57. Fault tolerance was only a manual runbook step
**Changed:** `scripts/recovery_test.py` (`docker compose run --rm recovery`)
kills Spark with SIGKILL under load, restarts it, measures time to the first
result and to a cleared backlog, and checks per product that every event
Kafka acknowledged was counted exactly once. Checked against a real Docker
engine: a container killed this way is not restarted by its restart policy,
so the outage lasts as long as the test says.

### 58. Kafka kept every event for 7 days
The broker default. A load test writes millions of events, all kept on disk
in the `kafka_data` volume for a week. **Changed:** `kafka-init` sets
`retention.ms` (24 hours, `KAFKA_RETENTION_MS`) on every start, so existing
topics get it too.

### 59. The first lag fix (item 54) never ran
Found on the live stack: lag still read exactly 0.0. The lag reader passed
`api_version_auto_timeout_ms`, which kafka-python 3 renamed to
`bootstrap_timeout_ms`, so building the client failed on every batch and the
listener quietly fell back to Spark's always-0 figure. **Changed:** the
settings live in `kafka_io.LAG_CONSUMER_CONFIG`, and a test checks every key
against `KafkaConsumer.DEFAULT_CONFIG`. Without the broker, lag is now
recorded as unknown rather than 0 (Spark's figure is used only when
`MAX_OFFSETS_PER_TRIGGER` caps batches), and a failed attempt is not retried
for 30 s, because each one blocks for about 5 s.

### 60. The load test crashed on start; the recovery test never saw "caught up"
Both found on the first real run. The load test passed `buffer_memory` to
`KafkaProducer`, a Java-client setting kafka-python 3 does not have. The
recovery test treated "caught up" as fewer than 500 unread events, but its
own traffic keeps flowing at 500 events/s, so a healthy query still has
(rate x batch time) events unread after every batch; it waited out its full
timeout and reported the catch-up time as unknown (the exactly-once checks
still passed). **Changed:** the setting is gone, and a test now parses every
`KafkaProducer`/`KafkaConsumer` call in the project and checks each keyword
against the installed library's `DEFAULT_CONFIG`. Caught up now means less
than one trigger interval's worth of traffic is waiting.

### 61. The first full load test called 1,000 events/s "not kept up"
It kept up at 2,500 to 10,000 events/s but not at 1,000, which made no
sense: that step cleared its backlog in 8 s and Spark read 1,020 events/s.
The "rising" check compared the first third of the step with the last, and
the first third still held the idle backlog from before the load started, so
the normal climb to a steady level looked like falling behind. **Changed:**
the first three batches of each step are ignored; the check compares halves
of what remains; and a step also fails if Spark read under 90% of what was
sent. Replayed on the shape of that run, the old rule says rising and the new
one does not. The same run showed 10,000 events/s was not the ceiling
(largest batch 9.5 s of a 10 s trigger), so the default steps now go to
20,000 events/s with six producer processes. Also: the tool containers mount
the code instead of needing `run --build`, which had rebuilt and restarted
the API before every test, and the load generator no longer prints an
idempotence warning per process.

---

# Eleventh pass - test tooling and CI

### 62. The tests only covered Spark, and CI barely ran
The suite tested the transforms and the streaming plan, but nothing ran the
API or the dashboard, and CI ran four lint rules and that one file.
**Changed:**
- `tests/test_api.py` - 23 tests driving the real FastAPI app over an
  in-memory MongoDB (mongomock): sums across windows, weak pairs hidden, the
  trending fallback, an unknown product, the ranking methods, graph edges,
  `/metrics` output including "unknown" lag, and a database failure returning
  503 rather than 500.
- `tests/test_dashboard.py` - Streamlit's `AppTest` runs the real page against
  that API, so a renamed field fails a test instead of the browser.
- `pytest` runs all three groups (74 tests); the Spark group still runs as a
  plain script inside the container, where pytest is not installed, and
  `tests/conftest.py` turns any failed `check()` into a failed pytest test.
- `pyproject.toml` holds the pytest, coverage, ruff and mypy settings.
- CI now runs ruff and mypy, the suite on Python 3.11 and 3.12 with a coverage
  summary, and `docker compose up` for the whole stack followed by the
  end-to-end check.

### 63. Lint and type checks had never been run properly
Turning on ruff's real rule set found 91 problems and mypy found 17.
**Changed:** all fixed - import order, outdated typing imports, `Optional`
defaults that PEP 484 forbids, a `zip()` without `strict=`, a `try/except/pass`
that hid errors, missing annotations on the module-level MongoDB clients and
aggregation pipelines. Ruff targets Python 3.10, the version in the Spark
image, so it never suggests syntax the container cannot run. Both are clean
and CI fails on either.

### 64. A clean machine could not run the tests at all
Found on the first run outside the development environment: `pytest` stopped
at collection with "the starlette.testclient module requires the httpx2
package". Starlette 1.6 needs `httpx2` for its test client, and the
development machine happened to have the older `httpx` from another project,
so nothing complained there. **Changed:** `requirements-test.txt` declares
`httpx2`, and a test checks that every package the tests import is declared.
PySpark is now pinned to the 4.1.3 series as well: the container runs 4.1.3,
and an unpinned install pulled 4.2.0, so a green test run would not have
meant the same code passes in the container.

### 65. The Spark tests could not run on a Windows host
On Windows, twelve Spark tests failed with `Python worker failed to connect
back`. The real cause was two lines further up in the stderr Spark captured:
`Python was not found; run without arguments to install from the Microsoft
Store`. PySpark starts one Python process per executor using whatever
`PYSPARK_PYTHON` says, defaulting to `python3` - a name that on Windows hits
the Store alias rather than the installed interpreter, and inside a virtualenv
on any OS can resolve to a different interpreter than the one running the
tests. **Changed:** `spark_session()` pins `PYSPARK_PYTHON` and
`PYSPARK_DRIVER_PYTHON` to `sys.executable` unless the environment already
sets them, and a new test runs a real Python worker and
checks it reports the driver's interpreter version, so a broken worker launch
fails with that sentence instead of a connection error.

### 66. A dependency's type stubs could stop the type check entirely
The first CI run failed in the lint job with
`numpy/__init__.pyi:737: error: Type statement is only supported in Python
3.12 and greater ... errors prevented further checking`. mypy is configured
for Python 3.10, the version the Spark image runs, so it refuses to parse
stubs written with PEP 695 `type` statements - and numpy 2.5, installed fresh
on the runner, writes 65 of them. The development machines had numpy 2.4 and
saw nothing. The project's own code was never checked at all that run.
**Changed:** `pyproject.toml` tells mypy not to follow numpy's stubs (nothing
here imports numpy; it arrives with pandas), and a test asserts both that
setting and the 3.10 target, so raising one without the other fails. Verified
by reproducing the exact error under Python 3.12 with numpy 2.5.3 and
watching it clear.
