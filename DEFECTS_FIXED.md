# Defects found in the original project, and what was done

> **Naming.** The project was renamed from *Real-Time E-Commerce
> Recommendation System* to **Streaming Product Affinity Pipeline** (see the
> Eighth pass). Entries written before the rename keep the old names
> (`/recommendations`, the `recommendations` collection, `recsys_` metrics)
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
product also viewed Y product"*, and that logic did not exist.

**Fixed:** a stream-stream self-join (first on `user_id`, later on the
browsing session, see ADR 0002) with a time constraint
produces real co-occurrence pairs, weighted by event type. See
`transforms.co_occurrence`.

### 2. Unbounded state (verified)
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
`producer.py` emitted product IDs 9001 to 9005. `ui/dashboard.py` emitted
101 to 105, which were the producer's **user** IDs. The dashboard matched
`product['id'] == product_id`, so an event from the producer could never
render. Users also differed: producer 101 to 105, dashboard hardcoded 1001.

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
The source held `# Task-1: Sort products in descending order based on event_count`, but the
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
branch reliably emits. The streaming query produced `(9001, 9003)` but never
its mirror.

**Fixed:** `mirror_pairs` is applied to the batch DataFrame inside
`foreachBatch`, where the union behaves normally.

### 12. A windowed aggregation on a derived time column is rejected
Bucketing on `least(l_time, r_time)` loses event-time attribution and Spark
rejects the aggregation with `STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION`.
Re-declaring the watermark does not work either: Spark 4 raises
*"Redefining watermark is disallowed"*.

**Fixed:** the window buckets on `l_time`, the join's left event time, which
still carries the watermark.

---

## Hygiene

| | |
|---|---|
| `db/mongo_config.py` was 0 bytes | removed, config lives in `src/common/config.py` |
| No `requirements.txt` | version ranges, verified (see #14) |
| No `README`, no `.gitignore` | both added |
| `__pycache__/` committed | git-ignored |
| Hostname/port hardcoded in five files | all environment-driven |
| Service named `mongo`, container `mongodb`, code used `mongodb` | one name throughout |
| ZooKeeper container | Kafka in KRaft mode (ZooKeeper is removed in Kafka 4.x) |
| No health checks, so Spark raced the broker | `condition: service_healthy` |
| No tests | 133 tests (Spark, API, dashboard, real data), plus CI on every push |


---

# Fourth pass: dependency and version portability

Found by installing the pinned requirements on a clean Python 3.12, not by
reading them.

### 13. `kafka-python==2.0.2` does not work on Python 3.12
Fails at import:

```
ModuleNotFoundError: No module named 'kafka.vendor.six.moves'
```

The producer, the dashboard and the smoke test all import it, so nothing that
touches Kafka would have started. **Fixed:** `kafka-python>=2.2`, verified
working on 3.0.11.

### 14. Pinning PySpark to the image version was wrong and fragile
`pyspark==3.5.3` was pinned to match `apache/spark:3.5.3`. That is
unnecessary. The host's PySpark is used only by the test suite, and the job runs
inside the container with the image's own PySpark. **Fixed:** relaxed to
`pyspark>=3.5,<5.0`, and the suite is verified on **both 3.5.3 and 4.2.0**.

### 15. The streaming test only passed on Spark 4
On Spark 3.5, a query with chained stateful operators (stream-stream join
followed by a windowed aggregation) uses a *global* watermark that lags one
micro-batch. Two input batches emit nothing and the window never closes. Spark 4
emits on two.

Diagnosed by instrumenting the watermark:

```
t=  5s rows=0 watermark=None
t= 10s rows=0 watermark=1970-01-01T00:00:00.000Z
t= 15s rows=2 watermark=2023-11-14T22:43:21.000Z
```

**Fixed:** the test feeds three batches, which works on both. This matters
beyond the test: it is why the pipeline needs a continuously running producer
to emit recommendations at all. A burst of events followed by silence leaves
the last window open.

### 16. The streaming test was non-deterministic
It broke out of its polling loop on the *first* rows to appear, which on 3.5
was a later batch's pair. It also assumed the file source reads files in
filename order, but it orders by modification time. **Fixed:** files are
written with a pause between them, and the test polls until the specific
expected pair appears.

### 17. `_updated_at` was never actually set
`row["_updated_at"] = row.get("_updated_at")` set the field to `None`, because the
key never existed. Dead code. **Fixed:** records the real UTC write time,
which is what makes the `/pipeline` processing-lag metric possible.


---

# Fifth pass: pre-release test

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
to a number. Verified on a live streaming query and covered by a test.

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
and outranked real lift values (e.g. 0.42). Two different scales were compared
as one. **Fixed:** defined scores rank first, and a test covers it.

### 24. The smoke test could not fail its DLQ check, and could time out
The check was `count >= 0`. It now sends a malformed event and requires the
dead-letter count to rise. The wait was 240 s, shorter than the worst case for
a 5-minute window plus the 2-minute watermark. It is now 540 s, with a
heartbeat event each poll so windows close even without the producer.

### 25. The README did not let a stranger run the project
No prerequisites, no clone step, no Windows commands (the Makefile does not
run in PowerShell), links to screenshots and a benchmarks file that did not
exist, two different wrong test counts, and a join snippet still keyed on
`user_id`. **Fixed.**


---

# Sixth pass: first full run on Windows

Found by running the whole stack on Windows 11 with Docker Desktop.

### 26. Recommendations took ~17 minutes to appear
With `COOCCURRENCE_WINDOW=5 minutes` and `CO_VIEW_GAP=10 minutes`, the
dashboard showed zero pairs long after trending was full. Spark 3.5 holds a
time-interval join's output back by the join's time range, so a pair is saved
only when events arrive about window + gap + watermark later. Measured with
a file-source stream: 5/10 minutes emitted at +17 minutes of event time, 1/2
minutes at +5. **Fixed:** defaults are now 1 minute and 2 minutes. A session
never spans more than ~100 s, so no pairs are lost. A test pins the defaults.

### 27. Seed events could be skipped entirely
The job read Kafka from `latest`, so events seeded while Spark was still
starting (exactly what `make demo` does) were never read. **Fixed:** a new
query starts from `earliest` (`STARTING_OFFSETS`), and restarts still resume from
the checkpoint. Seeding after the producer has started drops back-dated events
as late. That is now documented in the README, RUNBOOK and `seed.py`.

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
kafka-python 3.x deprecates lambda serializers, and PowerShell renders each
warning as a red error. **Fixed:** `src/common/kafka_io.py` provides real
`Serializer` classes, used by the producer, dashboard and all scripts.

### 31. Scripts looked frozen when their output was redirected
Python buffers stdout when it is piped, so `smoke_test.py | Out-File` showed
nothing for up to nine minutes. **Fixed:** the scripts line-buffer their
output.

### 32. `use_container_width` is deprecated in Streamlit
**Fixed:** `width="stretch"`, with `streamlit>=1.50` (verified on 1.50 and 1.64).

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
`requirements-spark.txt`: only pymongo, python-dotenv and kafka-python, which
is all the job and the tests import. Resolution checked for Python 3.8.

---

# Seventh pass: checking the live stack

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
README. The pop-up is harmless.

### 39. Chance pairs cluttered recommendations and the graph
Pairs seen twice with lift ~0.02 (MacBook to iPhone) appeared in both. They
came from seeded v1 events, which had no `session_id` and so paired by shopper
across separate visits. **Fixed:** seeded v1 events carry a session id, and
pairs seen fewer than `MIN_PAIR_COUNT` (3) times are left out.

---

# Eighth pass: rename and platform upgrade

### 40. The name promised something the project does not do
It was called a recommendation system, but nothing is personalised and nothing
is learned: it finds products viewed together in a session. **Changed:** the
project is the Streaming Product Affinity Pipeline.
`/recommendations/{id}` is `/related-products/{id}` (response key
`related_products`), the `recommendations` collection is `product_pairs`,
`RECOMMENDATION_LOOKBACK_MINUTES` is `PAIR_LOOKBACK_MINUTES` and the database
is `product_affinity`. Metrics use the `affinity_` prefix, the Spark query and
checkpoint are `product_pairs`, and containers are prefixed `affinity-`.

### 41. The Spark image ran an end-of-life Python
`apache/spark:3.5.3` ships Python 3.8, unsupported since October 2024.
**Changed:** Spark 4.1.3 (`apache/spark:4.1.3-python3`: Java 17, Scala 2.13,
Python 3.10) with the matching `spark-sql-kafka-0-10_2.13` connector. The Ivy
cache path is set explicitly because Spark 4 moved its default. Verified:
the full suite passes on PySpark 4.1.3, and the watermark delay (item 26) and the
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
optimizer normalises it). Measured over ten and twenty minutes of steady
sessions the join state levels off at ~600 rows and pairs are emitted on
time, so eviction works. **Changed:** only that logger is set to ERROR, and a
new test feeds ten minutes of sessions and fails if the join state keeps
growing.


---

# Ninth pass: realistic data, parallelism, one-command setup

### 44. The demo data never crossed categories
Every session picked all its products from categories related to the first
one (`catalog.AFFINITY`), so a shopper who looked at shoes never looked at a
phone. The graph showed perfectly separate clusters, which real clickstreams
never do, and lift had nothing to separate. **Changed:**
`catalog.session_products` sends each further view to any product with
probability `CROSS_CATEGORY_RATE` (default 0.15). The producer and the seed
both use it. Measured: related pairs score lift 6 to 14 and cross-category pairs
0.7 to 3.5, so the ranking still separates them. A test checks that a rate of 0
never crosses and a rate of 1 does. The dashboard graph draws cross-category
links dashed and has a slider for how many links to show. At the old fixed
limit of 30 the weakest cross links (shoes + phones) were never drawn.

### 45. One Kafka partition
Kafka auto-created `clickstream` with one partition, so Spark read it with one
task. **Changed:** auto-creation is off, and a `kafka-init` service creates the
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
`job.py` and crashed the job (see item 52).

### 49. Running the project needed four terminals and a host Python
**Changed:** the API, dashboard, producer, seed and a `smoke` tool service run
in Compose from one image (`Dockerfile.app`, non-root user). Start-up order is
declared with health and completion conditions. `seed --if-empty` skips the
backfill when results already exist, so restarting the stack does not inject
back-dated events that would be dropped as late.

### 50. Nothing alerted when the pipeline stopped
**Changed:** `/metrics` exports `affinity_pipeline_staleness_seconds` and
`affinity_processing_delay_seconds`, and `monitoring/alerts.yml` defines
`ApiDown`, `PipelineStale`, `KafkaLagGrowing`, `BatchSlowerThanTrigger` and
`StateStoreGrowing`.

### 51. The first one-command build failed on Docker Desktop
The four Python services shared one `image:` name. Compose builds them in
parallel, and with Docker Desktop's image store each build tried to tag the
same name: `image "streaming-product-affinity-app:latest": already exists`.
**Changed:** no shared name. Each service gets its own tag
(`streaming-product-affinity-api`, `-dashboard`, ...). The layers are
identical, so they are built and stored once. A test fails if the shared name
comes back.

### 52. The Spark job restarted every ~20 seconds (introduced by item 48)
The client cache first lived in `job.py`. `spark-submit` runs that file as
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

### 53. Three quarters of the Spark log was py4j chatter
Measured over 1 h 38 min of the running job: 3,562 of 4,752 lines were
`py4j.clientserver | Received command c on object id ...`, and the unit-test
log opened with ~40 Spark INFO start-up lines. **Changed:** the `py4j` logger
is set to WARNING in `job.py`, and the test session sets `spark.log.level` so
start-up is quiet. The same 1 h 38 min had no Spark errors and three one-off
start-up warnings.

---

# Tenth pass: measurement

### 54. Kafka lag was always 0
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

### 55. Write timestamps were taken before the batch was computed
`_updated_at` was set when `foreachBatch` started. Spark computes the batch
lazily after that, so freshness, processing delay and any latency measured
from it read too low by up to a batch duration. **Changed:** rows are stamped
when each chunk is written. A test runs a sink against a batch that takes
0.3 s to "compute" and checks the stamps come after it.

### 56. The load test measured the wrong things
Two-event sessions of fixed products from one process, "lag" taken from the
window-close delay, hardware left for the reader to fill in, and no latency.
**Changed:** rewritten (`scripts/load_test.py`,
`docker compose run --rm loadtest`) with realistic sessions from several
processes, Spark's per-batch records, real lag, a stated "kept up" rule, probe
latency, detected machine details, and a report section in `docs/BENCHMARKS.md`. See ADR 0010.

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

### 60. The load test crashed on start, and the recovery test never saw "caught up"
Both found on the first real run. The load test passed `buffer_memory` to
`KafkaProducer`, a Java-client setting kafka-python 3 does not have. The
recovery test treated "caught up" as fewer than 500 unread events, but its
own traffic keeps flowing at 500 events/s, so a healthy query still has
(rate x batch time) events unread after every batch. It waited out its full
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
the first three batches of each step are ignored, the check compares halves
of what remains, and a step also fails if Spark read under 90% of what was
sent. Replayed on the shape of that run, the old rule says rising and the new
one does not. The same run showed 10,000 events/s was not the ceiling
(largest batch 9.5 s of a 10 s trigger), so the default steps now go to
20,000 events/s with six producer processes. Also: the tool containers mount
the code instead of needing `run --build`, which had rebuilt and restarted
the API before every test, and the load generator no longer prints an
idempotence warning per process.

---

# Eleventh pass: test tooling and CI

### 62. The tests only covered Spark, and CI barely ran
The suite tested the transforms and the streaming plan, but nothing ran the
API or the dashboard, and CI ran four lint rules and that one file.
**Changed:**
- `tests/test_api.py`: 23 tests driving the real FastAPI app over an
  in-memory MongoDB (mongomock): sums across windows, weak pairs hidden, the
  trending fallback, an unknown product, the ranking methods, graph edges,
  `/metrics` output including "unknown" lag, and a database failure returning
  503, not 500.
- `tests/test_dashboard.py`: Streamlit's `AppTest` runs the real page against
  that API, so a renamed field fails a test instead of the browser.
- `pytest` runs all four groups (133 tests). The Spark group still runs as a
  plain script inside the container, where pytest is not installed, and
  `tests/conftest.py` turns any failed `check()` into a failed pytest test.
- `pyproject.toml` holds the pytest, coverage, ruff and mypy settings.
- CI now runs ruff and mypy, the suite on Python 3.11 and 3.12 with a coverage
  summary, and `docker compose up` for the whole stack followed by the
  end-to-end check.

### 63. Lint and type checks had never been run properly
Turning on ruff's real rule set found 91 problems and mypy found 17.
**Changed:** all fixed: import order, outdated typing imports, `Optional`
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
`PYSPARK_PYTHON` says, defaulting to `python3`. On Windows that name hits
the Store alias instead of the installed interpreter, and inside a virtualenv
on any OS it can resolve to a different interpreter than the one running the
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
stubs written with PEP 695 `type` statements. numpy 2.5, installed fresh
on the runner, writes 65 of them. The development machines had numpy 2.4 and
saw nothing. The project's own code was never checked at all that run.
**Changed:** `pyproject.toml` tells mypy not to follow numpy's stubs (nothing
here imports numpy, which arrives with pandas), and a test asserts both that
setting and the 3.10 target, so raising one without the other fails. Verified
by reproducing the exact error under Python 3.12 with numpy 2.5.3 and
watching it clear.

# Twelfth pass: real data

### 67. Every number the project quoted was about speed, not quality
Throughput, latency and recovery were measured. Whether the recommendations
were any good was not, and could not be: the generator's own `AFFINITY` table
decides which products co-occur, so measuring the pipeline against it would
score the pipeline on rediscovering a rule that was handed to it. **Changed:**
the same pipeline now also runs on RetailRocket (2.7M real events) through
`scripts/fetch_dataset.py`, `scripts/replay.py` and `scripts/evaluate.py`, and
`docs/EVALUATION.md` records hit-rate@10 on held-out days next to a bestseller
baseline. See [ADR 0011](docs/adr/0011-real-data-and-evaluation.md).

### 68. Three things would have made a naive replay meaningless
All three fail silently:
- **No session ids.** RetailRocket has only visitor ids. Keying co-occurrence
  on the visitor pairs products a shopper saw three weeks apart. Visits are now
  cut at 30 minutes of inactivity.
- **2015 timestamps.** Sent as they are, every window the pipeline wrote would
  land eleven years before the API's lookback: the dashboard would show
  nothing and the run would look like a pipeline failure. Timestamps are now
  mapped onto the replay's own clock, keeping order and relative spacing, and
  the replay reports the compression factor and warns when it would push a
  visit wider than the co-view gap.
- **Random event ids.** Replaying a slice twice would double-count everything.
  Ids are now derived from the session and position, so a repeat converges.

### 69. An evaluation harness that cannot say "worse" proves nothing
The first version scored only the pipeline and skipped cases where it had no
answer. That quietly turns "answers 5% of queries" into a good-looking
average. **Changed:** an empty answer is a miss and lowers coverage, which is
reported next to the hit-rate. The query product is dropped before the top-k is
taken, so echoing it back can never score. A test feeds the harness a
model that is wrong on purpose and asserts it lands below the baseline.

### 70. A replay silently lost its last two minutes of data
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

### 71. The replay's progress log reported today's date as the dataset day
Cosmetic, but it made the one line that shows where in the dataset a replay
has reached useless: it read the day back out of the wire event, whose
timestamp has already been mapped onto the replay clock, so it always printed
today. **Changed:** each row carries its original dataset time, and a test
asserts the reported day is the dataset's.

### 72. The evaluation reported 0.000% for a misconfigured serving layer
The first real evaluation run scored the pipeline at 0.000% with 0% coverage,
which looked like a catastrophic model and was nothing of the kind: the API
container was still serving the twelve-product demo catalogue, so every real
item id was a 404 and every query came back empty. The run even wrote that
zero into `docs/EVALUATION.md`. **Changed:** when more than half the query
products come back unknown, the script now fails with the exact command that
fixes it (setting `CATALOG_FILE`) and writes no report, because a serving layer that
does not recognise the products is a misconfiguration, not a measurement. A
test drives the whole script against an API that 404s everything and asserts
it exits non-zero, names `CATALOG_FILE`, and leaves no report behind.

### 73. The dashboard crashed on the first real catalogue
`TypeError: unsupported format string passed to NoneType.__format__` at
`dashboard.py:238`, the moment the stack was pointed at a catalogue built from
RetailRocket. Real items have no price (the dataset hashes its properties, so
the catalogue honestly carries `None`), and the product caption formatted it
with `:,`. The dashboard tests never caught it because they only ever ran
against the demo catalogue, where every product has a price. **Changed:** the
caption shows the category alone when there is no price, and a test runs the
real page against a 5,000-item catalogue with no prices.

### 74. The page would have rendered 50,743 products
The same run would have drawn two buttons for every catalogue item and offered
a 50,000-entry select box. **Changed:** the click-to-send strip shows the
first 12 and says how many exist. The "Related products" chooser offers what
is currently trending (products that actually have data) plus a bounded slice
of the catalogue. Asserted in the same test.

### 75. A handled 404 looked like a crash in the dashboard tests
Found while writing the test above: the fake `requests.get` used by the
dashboard tests raised httpx's `HTTPStatusError`, which the dashboard does not
catch, so a 404 the real page handles quietly appeared as an unhandled
exception. The test harness was lying about the library it stood in for.
**Changed:** the fake raises `requests.HTTPError` like the real thing, and the
page now says "No data for this product yet" instead of rendering nothing.

### 76. The graph caption explained the demo generator as if it were the data
On a real catalogue the "Products viewed together" caption still said dashed
lines were "shoppers wandering, which the demo data does in about 15% of
views". That is a statement about the synthetic generator, printed underneath real
RetailRocket pairs. **Changed:** with a real catalogue the caption says what
the data actually supports (solid lines join two products from the same
category, dashed lines cross categories) and the demo explanation appears only
with the demo catalogue. Asserted in the real-catalogue dashboard test.

### 77. Two endpoints answered from a window they did not report
Found by asking why "Trending now" was empty while the graph beside it was
full. `/trending` honestly reports the last 30 minutes, and after a finished
replay that really is empty. But:
- `/related-products` widened its query to the whole retained history whenever
  the 30-minute lookback came back empty, while still reporting
  `"lookback_minutes": 30`. A caller could not tell a live answer from an
  hours-old one. Worse, it then divided those all-history pair counts by
  30 minutes of per-product marginals, so any lift computed on that path was
  meaningless.
- `/graph` applied no time filter at all: it summed every window still
  retained. A 30-minute "trending" panel and an all-time affinity graph sat
  side by side on the same page, both unlabelled.
**Changed:** both endpoints report `window` (`recent` or `all_retained`) and
set `lookback_minutes` to null when they widened. The graph takes the same
lookback as related-products, with a `minutes` override. Lift marginals are
computed over whichever window actually answered, and the dashboard says when
it is showing retained history instead of live activity. Four tests cover
both endpoints and the page.

### 78. A dashboard audit against real data: four more
Asked to check the whole page instead of the one crash, with the page
rendered against a database seeded to match a finished replay:
- **Every node in the graph was grey.** Node colour came from a six-entry map
  of demo category names, so a real dataset's `cat-1091` fell through to
  "unknown" and the graph's only grouping was lost. Colours are now assigned
  per graph, by position, not by hashing the name. A hash collides,
  and two categories sharing a colour makes the legend say something the
  picture does not.
- **The legend advertised categories that were not there.** It always listed
  the demo's six (Laptops, Phones, ...), whatever was on screen. It now lists
  the categories the graph actually contains, with "+ N more" past eight.
- **"Events in the last full minute" was 45 minutes old.** The metric took the
  newest completed window, whatever its age, so a stopped pipeline read as a
  running one. It now says "newest full minute" with the window's time and age
  when the data is not current.
- **The throughput caption claimed "Last 6 minutes ... the last bar is the
  minute still in progress."** Both false after a replay. It now states the
  window range, how long ago it ended, and only claims a bar is in progress
  when one actually is.
Also: `/trending` returned an empty list with no explanation when rows existed
but none inside the lookback, and the page said "No data yet" next to a
sidebar counting 82,490 rows. The API now says "No events in the last 30
minutes. The newest window ended 45 minutes ago." Six tests cover these.

### 79. Documentation that had drifted from the code
A full audit of README.md against the source, prompted by "check everything":
- "Every sink upserts on a natural key (`window_start, window_end,
  product_id`)" was wrong for two of the three sinks: pairs key on four fields
  (the related product too), and the dead-letter sink appends rather than
  upserting.
- The reproduce command said `--test-days 1` while the quoted numbers came
  from a 7-day test. The command, the Compose comment and the Makefile now
  all say 7, so running the documented line reproduces the documented table.
- The `/graph` row listed two of its four query parameters. The
  `/related-products` example response was missing `ranked_by`, `window`,
  `lookback_minutes`, `count` and four per-row fields, and nothing documented
  the widened `all_retained` window at all.
- "Three panels" described a page that has six sections, and left out
  click-to-send and the graph legend entirely.
- The Layout tree omitted `src/common/scoring.py`, `tests/conftest.py` and the
  `Makefile`, each referenced elsewhere in the same README.
- `results/evaluation.json` was cited as though committed. It is git-ignored
  and written locally, and the README now says so.

### 80. A test that failed about one run in fifteen
`test_pipeline_reports_how_far_behind_the_last_write_was` built a row one
minute old and stamped it "written four seconds after the window closed".
That lands in the future whenever the test starts in the first four seconds
of a minute, making staleness negative and the assertion fail. It had been
passing by luck. **Changed:** the window is two minutes old, so the write time
is in the past at every second of the clock while still inside the 120-second
freshness threshold. Run repeatedly to confirm.

### 81. The pair-table score decayed with the clock
`--source mongo` answered only from the recent lookback, so the same data
scored 7.9% twenty minutes after a replay and 0% an hour after it. The
measurement depended on when it was run, which makes it not a measurement.
**Changed:** it now mirrors what the API does, answering from the lookback
where there is something there and widening to everything retained when there
is not, and it counts which of the two answered
(`co_occurrence_recent` / `co_occurrence_retained`). `--strict-lookback`
keeps the old behaviour for anyone who wants only the live window. A test
scores five-hour-old pairs both ways.

### 82. "Day zero" was wherever the export happened to begin
The 30-day evaluation failed with "no test traffic in that window", and the
cause was two assumptions that RetailRocket's export does not honour: its rows
are not in time order, and its first row is 2015-06-02 while its earliest
event is 2015-05-03, seven weeks earlier.
- `split()` treated the first row as day zero, so seven weeks of May traffic
  were skipped as "before the window".
- It also stopped reading at the first row past the requested window. On a
  sorted file that is an optimisation. On this one it truncated the read, and
  at 30 days it stopped before reaching any test traffic at all.
**Changed:** both `scripts/evaluate.py` and `scripts/replay.py` now take day
zero from the dataset's earliest event (`retailrocket.time_span`, one scan),
neither stops early, and both log the span they found and the window they are
using. A test builds an export whose first row is 90 days in the future and
asserts the split still finds its test traffic.

The 7-day numbers already published were measured with the old anchor, so
they describe 2015-06-02 onwards rather than the dataset's first week. They
are being re-measured rather than reinterpreted.

### 83. The widened window did not survive a real dataset
The graph went blank after the 30-day replay: 1.5M pair rows, and the
"widen to everything retained" path I had added grouped all of them with no
time filter and no index to help, taking longer than the dashboard's
five-second timeout, which the page reported, correctly but uselessly, as
"No product pairs yet".
**Changed:** when the recent lookback is empty, both `/graph` and
`/related-products` now anchor the same lookback on the newest data that
exists (`window: "latest_available"`, with `as_of` so the caller sees how old
it is) instead of dropping the time bound. That uses the `window_start` index,
bounds the scan to one lookback's worth of rows, and answers a meaningful
question ("the most recent 30 minutes of data there is") instead of an
all-time popularity chart. A test proves rows outside that band stay out.

### 84. The graph query outran the dashboard's timeout
Measured, not guessed: `/graph` took **5.1 to 6.4 seconds** against a replayed
month, and the dashboard gave it five. So `requests.get` aborted, the page
fell through to "No product pairs yet", and a database holding 1.24M pair rows
looked empty. The endpoint itself was fine: the same query returned real
edges when asked directly.

The cost is inherent: the graph groups every pair row inside the lookback, and
a replay compresses a month into ten minutes of pipeline time, so the whole
month sits inside one 30-minute window. **Changed:** the API caches the graph
for `GRAPH_CACHE_SECONDS` (30 by default, its own TTL because it is far more
expensive than the other reads), and the dashboard gives that one call thirty
seconds with a spinner instead of five seconds and a misleading message. Two
tests: one asserts the caching, one asserts a different question is a
different cache entry.

### 85. The evaluation could score the pipeline on its own training days
The worst defect in the project so far, because it produced a *better* number
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

**The 35.1% figure is withdrawn.** It appears nowhere in the README or in
`docs/EVALUATION.md`. The published numbers are the 30-day ones in the README and `docs/EVALUATION.md`.

### 86. On real data, the graph's colours contradicted its lines
A 30-day RetailRocket replay put **31 categories** on the 30-line graph, and
the dashboard coloured them from a 12-colour palette by wrapping round it. So
11 colours each stood for two or three unrelated categories: Items 274435,
257597 and 369447 were all the same gold while the dashed lines between them
said "different categories". The legend showed eight of the 31 and named
colours that also meant something else.

More colours would not fix it. Even counting only categories with two or more
products on the graph there were 17, and 32 at 70 lines, beyond what anyone can
tell apart. **Changed:** up to 12 categories, each gets its own colour and the
legend names every one (it now lists up to 12, not 8). Past 12, no category is
coloured: every box is one neutral slate, its category is printed under its
id, and the legend says why. The solid/dashed line style already carries
same-or-different category, so nothing is lost and the picture can no longer
disagree with itself. The demo keeps its hand-picked colours. One new test
covers both regimes and the label.

### 87. Thirty pair counts printed on the lines were unreadable
With thirty lines, the count on each one overlapped its neighbours and the
product boxes, so the numbers the graph exists to show could not be read.
**Changed:** when Graphviz is installed (the container image now installs it),
the dashboard draws the graph to SVG on the server and embeds it with a small
script: no counts on the lines, and pointing at a line shows a translucent box
naming both products and how many times they were seen together. The line
under the pointer darkens, and a wide invisible copy of each line makes the
thin dashed ones easy to hit. The box is looked up by the edge's own id, so it
always describes the line under the pointer. Without Graphviz the page falls
back to the old chart with counts on the lines. Two tests: the hover table
matches the drawn edges exactly and no line carries text. A product name
cannot close the script early.

### 88. "Last update: 3649 s ago"
After a finished replay the dashboard counted the time since the last result
in raw seconds, which stops being readable after a minute. **Changed:** it
now reads like a clock ("42 s ago", "6 min ago", "1 h 2 min ago", "3 days
ago"), with a test for each step.

### 89. The hover graph shrank real data until it could not be read
The hover version of the graph sat in a frame of fixed height (560 px), and
the drawing scales to fit it. A 51-product real-data graph came out as a small
square in the middle of the column with labels too small to read. **Changed:**
the frame height now comes from the drawing's own proportions at the column's
usual width, clamped between 360 and 1000 px, so the graph fills the width it
has. One test covers the square, wide, tall and missing-viewBox cases.

### 90. The evaluation compared the pipeline with a baseline that was too easy
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
