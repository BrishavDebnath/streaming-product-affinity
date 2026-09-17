# Streaming Product Affinity Pipeline

*A streaming data-engineering project: Kafka 4 -> Spark 4 Structured Streaming
-> MongoDB -> FastAPI. It finds products that shoppers view together in the
same session, using co-occurrence statistics - no machine learning yet. See
[Scope](#scope) before reading further.*

Clickstream events flow through **Kafka** into **Spark Structured Streaming**,
which computes windowed **trending products** and **co-viewed product pairs**
("shoppers who viewed A in a session also viewed B"), writes them idempotently
to **MongoDB**, and serves them over **FastAPI** with a **Streamlit**
dashboard and **Prometheus/Grafana** monitoring.

```mermaid
flowchart LR
    P[producer<br/>synthetic sessions] -->|clickstream| K[(Kafka 4<br/>6 partitions)]
    S[seed<br/>20 min of history] --> K
    D[Streamlit dashboard] -->|your clicks| K
    K --> J
    subgraph J[Spark 4 Structured Streaming]
        V[parse + validate] --> T[trending<br/>1-min windows]
        V --> PP[product pairs<br/>stream-stream join]
        V --> DL[dead letter]
    end
    T --> M[(MongoDB)]
    PP --> M
    DL --> M
    M --> A[FastAPI]
    A --> D
    A -->|/metrics| PR[Prometheus + alerts] --> G[Grafana]
```

Every box is a container. One `docker compose up` starts them all.

---

## Scope

This is a **streaming data-engineering** project. The "related products"
layer is deliberately simple: session-scoped co-occurrence ranked by affinity,
lift or PMI - the same family as classic item-to-item "viewed together"
features.

What it does **not** do, and does not claim to:

- **No personalisation.** `/related-products/{product_id}` answers "what goes
  with this item", not "what should *this shopper* see next". There is no user
  model, so nothing here is called a recommendation.
- **No machine learning.** Lift is a statistic, not a trained model. ML is
  planned - see [Roadmap](#roadmap).
- **No offline evaluation.** The events are synthetic, so precision@k against
  the generator's own affinity table would measure nothing. Real evaluation
  needs real clickstream data.

The engineering is where the work is: bounded streaming state, idempotent
writes, dead-letter handling, schema-version coexistence, and a tested
transform layer. Those are all demonstrated and measured below.

<!--
## Demo

Capture these while the stack is running (see docs/screenshots/README.md),
save them in docs/screenshots/, then remove this comment wrapper.

![Dashboard](docs/screenshots/dashboard.png)

![Affinity graph](docs/screenshots/affinity-graph.png)
-->

## Prerequisites

| | |
|---|---|
| **Docker Desktop** (or Docker Engine with Compose v2.24+) | runs everything |
| **Git** | to clone the repository |
| **Python 3.11 or 3.12** *(optional)* | only to run scripts or tests directly on your machine |

Give Docker at least **8 GB of memory** (Docker Desktop on Windows uses half
your RAM by default). The Spark driver is set to 4 GB and MongoDB's cache to
1 GB. The first build downloads the Spark 4.1 image, the Kafka connector JARs
and the Python packages, so expect several minutes.

Ports used: `8501` dashboard, `8000` API, `3000` Grafana, `9090` Prometheus,
`4040` Spark UI, `9092` Kafka, `27018` MongoDB.

## Quick start

The same commands work in PowerShell, macOS and Linux.

```bash
git clone https://github.com/BrishavDebnath/streaming-product-affinity.git
cd streaming-product-affinity
docker compose up -d --build
```

That starts, in order:

1. **Kafka** and **MongoDB**, then **kafka-init**, which creates the
   `clickstream` topic with 6 partitions.
2. The **Spark** job, the **API** and **Prometheus/Grafana**.
3. **seed** - backfills 20 minutes of past sessions (skipped if MongoDB already
   has results), so the dashboard is not empty.
4. The **producer** - live synthetic traffic, about 20 events/s - and the
   **dashboard**.

Then open:

| | |
|---|---|
| Dashboard | http://localhost:8501 |
| API docs | http://localhost:8000/docs |
| Grafana | http://localhost:3000 |
| Prometheus (targets, alerts) | http://localhost:9090 |
| Spark UI | http://localhost:4040 |

**Expect trending within a minute and live product pairs after about five.**
The stream-stream join holds its output back by `CO_VIEW_GAP`, so a pair is
saved only once events arrive roughly `COOCCURRENCE_WINDOW + CO_VIEW_GAP +
WATERMARK` later (1 + 2 + 2 minutes by default). Raising the gap to 10 minutes
pushes that to ~17 minutes; this was measured on Spark 3.5 and 4.1, not
estimated. Keep the producer running: windows only close when newer events
arrive.

## Everyday commands

| | |
|---|---|
| Status | `docker compose ps` |
| Logs | `docker compose logs -f spark` (or `api`, `producer`, ...) |
| End-to-end check (up to ~9 min) | `docker compose run --rm smoke` |
| Throughput and latency benchmark (~25 min) | `docker compose run --rm loadtest` |
| Crash-recovery test (~7 min) | `docker compose run --rm recovery` |
| Unit tests (Spark container) | `docker compose run --rm --no-deps spark /opt/spark/bin/spark-submit /app/tests/test_transforms.py` |
| Stop, keep data | `docker compose down` |
| Stop and delete all data | `docker compose down -v` |

Settings live in `.env` (copy `.env.example`); every value has a default.
After changing one, run `docker compose up -d` again. Changing
`KAFKA_PARTITIONS`, `SHUFFLE_PARTITIONS`, `STATE_STORE` or a window size needs
a fresh start (`docker compose down -v`), because Spark will not resume from a
checkpoint whose plan has changed.

A `Makefile` wraps the same commands for macOS and Linux (`make up`,
`make smoke`, `make test`, `make loadtest`, ...).

### Running scripts on your machine (optional)

Only needed for development. The containers already publish Kafka on
`localhost:9092` and MongoDB on `localhost:27018`.

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows;  macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env              # Windows: copy .env.example .env
# PowerShell: $env:PYTHONPATH = "."   macOS/Linux: export PYTHONPATH=.
python scripts/smoke_test.py
```

Running the unit tests natively needs Java 17+ as well:
`pip install -r requirements-test.txt`, then `python tests/test_transforms.py`.

---

## How product pairs are found

The core is a **stream-stream self-join** on the browsing session
(`session_id`), constrained so both events fall within `CO_VIEW_GAP` of each
other. It originally joined on `user_id`; with users continuously active that
paired everything with everything ([ADR 0002](docs/adr/0002-session-keyed-cooccurrence.md)).
Events without a session id fall back to a key derived from the user.

```python
left.join(right,
    (col("l_session") == col("r_session"))
    & (col("l_product") < col("r_product"))          # each pair once
    & (col("r_time") >= col("l_time") - expr("INTERVAL 2 minutes"))
    & (col("r_time") <= col("l_time") + expr("INTERVAL 2 minutes")))
```

Four details matter:

- **The time constraint is what bounds the join state.** Without it Spark must
  retain every event forever, in case a future event joins to it. It also
  delays output by the same amount, which is why the gap is kept short.
- **`l_product < r_product`** means (A,B) and (B,A) are one row. The mirror is
  written at sink time so a lookup on either product finds it.
- **Weak pairs are hidden.** A pair seen fewer than `MIN_PAIR_COUNT` (3)
  times in the lookback is left out of related products and the graph.
- **Pairs are weighted, not counted.** A `purchase` is worth 5.0 and a `search`
  0.5 (`config.EVENT_WEIGHTS`); pair affinity is the geometric mean, so one
  strong plus one weak signal ranks below two strong ones.

### Where the structure in the demo data comes from

The data is synthetic, and its structure is **designed**, not discovered.
`catalog.session_products` generates each visit: the first product is random,
and each further product comes from a related category (`AFFINITY` in
`src/common/catalog.py`: laptops with laptop accessories, phones with phone
accessories and audio, footwear with footwear) - or, with probability
`CROSS_CATEGORY_RATE` (15%), from any category, because real shoppers wander.

The streaming job knows nothing about categories. It only sees which products
appeared in the same session, so the clusters on the dashboard are the
pipeline **rediscovering** a rule that was put into the data - a known-answer
test of the pipeline, not a finding about shoppers. With 15% wandering,
simulated related pairs score a lift of about 6-14 and chance pairs about
0.7-3.5, which is exactly the separation lift is meant to provide. Real
clickstream data is on the [Roadmap](#roadmap).

Uniform random events would make every pair equally likely and leave nothing
to find; `docker compose run --rm smoke` checks that laptops really do pair
with laptop sleeves.

---

## Bounded state

Every stateful operation is windowed. This is not stylistic. A streaming
aggregation grouped only by business keys retains state for every key it has
ever seen — the watermark cannot evict it, because `event_time` is not part of
the grouping key. Measured on a rate source at 200 rows/s:

| | state rows over time |
|---|---|
| `groupBy("user_id", "product_id")` — no window | 0 → 1600 → 2400 → **3200** in 12 s, growing linearly |
| `groupBy(window(event_time, ...), key)` | **flat at 100** across 45 s |

The first is a slow memory leak that looks fine in a five-minute demo and
takes the job down in production.

`tests/test_transforms.py` asserts `numRowsTotal` stays bounded in a real
streaming query.

---

## Correctness properties

**Idempotent writes.** Every sink upserts on a natural key
(`window_start, window_end, product_id`), so replaying a batch after a failure
converges rather than duplicating. Unique indexes enforce this at the database
level too.

**Nothing is silently dropped.** Events failing validation are routed to a
dead-letter collection with the reason and the original payload, instead of
becoming nulls. The producer emits malformed events at `MALFORMED_RATE` so the
path is continuously exercised.

**Exactly-once results across a crash.** `docker compose run --rm recovery`
kills the Spark container mid-stream with SIGKILL, restarts it, and checks
that every event Kafka acknowledged was counted exactly once. Spark resumes
from the offsets and state in its checkpoint; a batch cut short by the kill is
re-run, and the upserts make the re-run harmless.

**Cold start is answered explicitly.** A product with no co-occurrence data
yet returns trending products, and the response says
`"source": "trending_fallback"` rather than pretending the two are the same.

---

## Measured performance

Measured on an 8-core i5-13450HX with 12 GB of memory given to Docker, with
Kafka, Spark, MongoDB and the load generator all sharing those cores. Full
results, and how to reproduce them: [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

- **15,000 events/s sustained.** Spark read every event, the backlog stayed
  flat and cleared 16 s after the load stopped. At 20,000 events/s it falls
  behind: batches take 15.6 s against a 10 s trigger.
- **An event reaches its trending row in about 8 s** (median) at 2,500-5,000
  events/s, and in 15 s at the 95th percentile at 15,000 events/s. Most of
  that is the wait for the next 10 s trigger.
- **A product pair appears about 5 minutes after the events**, by design: its
  window, the co-view gap and the watermark all have to pass first.
- **Crash recovery:** Spark killed with SIGKILL under load wrote its first new
  result 12 s after restarting and cleared the 38,471-event backlog 15 s after
  restarting. All 120,224 events were counted exactly once - none lost, none
  double-counted.
- **The co-occurrence join held 4.7M events at 15,000 events/s** and stayed
  bounded: RocksDB keeps that off the JVM heap, and the 2-minute co-view gap
  is what caps it (`results/load_test.csv`).

Two scripts produce these numbers against the running stack:

- **`loadtest`** ramps the event rate (2,500 to 20,000 events/s by default,
  90 s each) with six producer processes sending realistic sessions. For
  each step it records what Spark itself reported (events read, batch time,
  unread backlog) and whether Spark kept up: the backlog must not climb during
  the step and must clear within two trigger intervals afterwards. Probe
  events with unique product ids time the path from sending an event to its
  row appearing in MongoDB.
- **`recovery`** kills Spark under load and measures how quickly it resumes
  and whether any event was lost or double-counted.

Everything shares one machine's cores, so the figures describe a laptop, not
a cluster. See [ADR 0010](docs/adr/0010-measuring-the-pipeline.md) for why
the measurements are taken this way.

---

## Tested

The suite runs **150 checks** against a real local `SparkSession`. It needs
no Kafka and no Mongo; run it inside the Spark container with the unit-test
command above.

- parsing, type coercion, weighting, and rejection reasons
- garbage payloads route to the DLQ rather than crashing the job
- trending counts, weighted scores, distinct users, window separation
- co-occurrence finds co-viewed pairs, ignores events outside the gap, never
  pairs a product with itself, and never pairs two different users
- a **genuine streaming query** — file source → watermark → stream-stream
  join → windowed aggregation → memory sink — asserting the pair is emitted,
  that counts aggregate across users, and that state stays bounded
- ten minutes of steady sessions, showing the join state stops growing once
  the co-view gap and watermark have passed
- lift and PMI maths, and that pairs with an undefined lift rank below real ones
- the monitoring listener can be built, reads Spark's progress objects, and
  measures Kafka lag against the broker rather than Spark's planning snapshot
- sinks stamp rows at the moment they are written, in bounded chunks, and
  their writers can be shipped to Spark's Python workers
- the benchmark helpers: percentiles, the kept-up rule, report sections, and
  Docker's timestamps
- the dashboard reads only fields the API actually returns
- the session-generation rule, the RocksDB state store, and that Compose
  starts every service in a safe order with Prometheus loading its alerts

The transforms are pure `DataFrame -> DataFrame` functions in
`src/streaming/transforms.py` precisely so this is possible. Streaming logic
that can only be verified by watching a dashboard cannot be refactored safely.

CI runs the same suite on every push (`.github/workflows/ci.yml`).

---

## API

| endpoint | purpose |
|---|---|
| `GET /health` | liveness + MongoDB reachability |
| `GET /throughput?windows=` | events per window — the pipeline's own measured rate |
| `GET /pipeline` | processing lag: window close → row written |
| `GET /graph?limit=` | co-occurrence as nodes and edges |
| `GET /trending?limit=&minutes=` | top products over the last N minutes, by weighted score (cached `CACHE_TTL_SECONDS`) |
| `GET /related-products/{id}?limit=&score_by=` | co-viewed products ranked by `affinity`, `lift` or `pmi`, with trending fallback |
| `GET /stats` | collection counts, latest window, request counters |
| `GET /metrics` | Prometheus text format |

```bash
curl localhost:8000/related-products/9001 | jq
```

```json
{
  "product_id": 9001,
  "source": "co_occurrence",
  "related_products": [
    {"product_id": 9003, "affinity": 42.6, "pair_count": 18,
     "name": "Laptop Sleeve 13\"", "price": 1499, "category": "laptop-acc"}
  ]
}
```

---

## Dashboard

Three panels exist to show the pipeline rather than to decorate the page:

- **Events per minute** — charted from what Spark actually wrote to MongoDB,
  not from a producer-side counter. If the producer is sending but this is
  flat, the bottleneck is downstream of Kafka.
- **Processing delay** — `written_at - window_end`, i.e. how long after a
  one-minute window ends its final numbers are saved. A few seconds up to the
  trigger interval is healthy; a steadily rising number means the job cannot
  keep up.
- **Products viewed together** — the product pairs as a graph: line thickness
  by affinity, box colour by category. Solid lines join categories the demo
  data links (laptop + sleeve), dashed lines join the rest (shoes + phone):
  shoppers wandering across categories (see above). A slider shows more of
  the weaker links. The line style is display only - the pipeline never sees
  categories. A thick dashed line such as phone accessories + audio is a link
  the pipeline found indirectly: both are viewed with phones.

The graph is rendered with `st.graphviz_chart` from a generated DOT string, so
it needs no extra dependency — no networkx, no pyvis, no plotly.

## Layout

```
docker-compose.yml         the whole stack; Dockerfile.app / Dockerfile.spark build it
src/common/config.py       every tunable, from environment variables
src/common/catalog.py      product catalogue and the session-generation rule
src/common/kafka_io.py     Kafka serializers shared by every producer
src/common/mongo.py        one reused MongoDB client per process
src/streaming/transforms.py  pure, unit-tested Spark transformations
src/streaming/job.py       job wiring, idempotent Mongo sinks, indexes, metrics
src/producer/producer.py   live session generator
src/api/main.py            FastAPI serving layer and /metrics
src/ui/dashboard.py        Streamlit dashboard
scripts/seed.py            backfill of past sessions
scripts/smoke_test.py      end-to-end assertion against a running stack
scripts/load_test.py       throughput and latency benchmark
scripts/recovery_test.py   crash-and-restart test
src/common/bench.py        shared benchmark helpers (standard library only)
monitoring/                Prometheus config, alert rules, Grafana dashboard
tests/test_transforms.py   the test suite
```

Configuration is entirely environment-driven: the same code runs in the
containers (`kafka:29092`) and on your machine (`localhost:9092`).

---

## Monitoring

Prometheus (`:9090`) and Grafana (`:3000`) start with the rest of the stack.
Grafana opens straight on the **Streaming Product Affinity Pipeline**
dashboard - read-only, no login - charting Kafka consumer lag, ingest vs
processing rate, batch duration, state-store size (each per Spark query), the
related-products source mix, and the dead-letter count.

`/metrics` reports the value *now*; Prometheus scrapes the API container
(`api:8000`) every 10 s and keeps the history; Grafana draws it.

**Kafka lag** is the number of events a query has not read yet, measured
after each batch against the broker's latest offsets. Spark's own figure
(`maxOffsetsBehindLatest`) compares with the offsets it saw when it *planned*
the batch, so without a batch-size cap it is always 0. A sawtooth up to
(event rate x trigger interval) is normal; a rising floor means falling
behind.

**Alert rules** (`monitoring/alerts.yml`) are evaluated by Prometheus and
listed at http://localhost:9090/alerts: API down, pipeline stale for 2
minutes, Kafka lag above 1000 and rising, batches slower than the trigger
interval, and state that keeps growing. There is no Alertmanager, so nothing
is sent anywhere - the page shows what would page someone.

Grafana may flash an "Unauthorized" pop-up: its page asks the server who is
signed in (`/api/user`), and that request is refused for anonymous visitors.
It is harmless.

## Documentation

| | |
|---|---|
| [docs/adr/](docs/adr/) | 10 architecture decision records — what was chosen, and what was rejected |
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | fault-tolerance demo, load testing, diagnosing lag |
| [docs/BENCHMARKS.md](docs/BENCHMARKS.md) | measured throughput, latency and crash recovery, written by the benchmark scripts |
| [DEFECTS_FIXED.md](DEFECTS_FIXED.md) | every defect found and how it was verified |

## Licence

MIT - see [LICENSE](LICENSE).

## Known limitations

1. **Co-occurrence is not collaborative filtering.** No matrix factorisation,
   no embeddings, no personalisation to a specific user's history. It answers
   "what is viewed with this" rather than "what should *you* see next".
   See the [Roadmap](#roadmap).
2. **No offline evaluation.** There is no held-out set and no precision@k,
   because the events are synthetic — the ground truth would be the generator's
   own affinity table, which would measure nothing. Real evaluation needs real
   clickstream data.
3. **Single-broker Kafka.** Six partitions let Spark read in parallel, but
   with one broker nothing is replicated; broker failure and partition
   rebalancing are untested.
4. **Local Spark only.** `local[8]` in one container, never run on a real
   cluster, so executor tuning and network shuffles are untested.
5. **No authentication** on the API or the dashboard.
6. **Benchmarks are from one laptop.** Kafka, Spark, MongoDB and the load
   generator share the same cores, so the numbers show relative behaviour
   and where this setup saturates, not what a cluster would do.

## Roadmap

Planned, **not implemented**. The pipeline is being shaped so these can be
added without rewriting what exists.

- **Real data and evaluation** - replay the public RetailRocket clickstream and
  measure hit rate@10 against a bestseller baseline.
- **Machine learning (later)** - the ranking methods are designed to be
  pluggable, so learned models can sit beside co-occurrence and be compared on
  the same evaluation: item2vec embeddings trained on sessions (Spark MLlib),
  ALS on implicit feedback, a session-based sequence model (e.g. SASRec),
  experiment tracking with MLflow, and optional LLM-written explanations.
