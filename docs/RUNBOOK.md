# Runbook

Every command runs from the project folder. Nothing needs to be installed on
the host except Docker.

## Verify fault tolerance (do this once, and record it)

This is the demo that proves checkpointing is understood rather than merely
configured.

```bash
docker compose up -d --build                  # let it run ~3 minutes
curl -s localhost:8000/stats

docker compose kill spark                     # hard kill, mid-stream
sleep 30                                      # the producer keeps sending
docker compose up -d spark                    # restart

docker compose logs -f spark                  # watch it resume
curl -s localhost:8000/stats
```

**Expected:** the job resumes from its checkpointed Kafka offsets (kept in the
`spark_checkpoints` volume). No gap in window coverage, and no duplicated
counts, because the sinks upsert on a natural key (ADR 0004). While Spark is
down, http://localhost:9090/alerts shows `PipelineStale` going from pending to
firing after about three minutes, and clearing once Spark catches up.

## Measure throughput

```bash
python scripts/load_test.py --rates 100 500 1000 2500 5000 --seconds 60
```

Writes `docs/BENCHMARKS.md` and `results/load_test.csv`. The highest rate at
which lag stays flat is the sustainable throughput; once lag climbs batch over
batch and never recovers, the pipeline is falling behind.

## Alerts

Rules live in `monitoring/alerts.yml`; Prometheus shows their state at
http://localhost:9090/alerts.

| Alert | Fires when | First thing to check |
|---|---|---|
| `ApiDown` | Prometheus cannot scrape the API for 1 min | `docker compose logs api` |
| `PipelineStale` | Spark has saved nothing for 2 min | `docker compose ps spark`, then its log |
| `KafkaLagGrowing` | over 1,000 events behind and still growing | see *Diagnose: lag climbing* |
| `BatchSlowerThanTrigger` | batches take over 10 s for 5 min | same |
| `StateStoreGrowing` | join or window state keeps growing for 15 min | ADR 0001 |

## Diagnose: no related products appearing

1. `curl localhost:8000/stats` - is `product_pairs` non-zero?
2. If zero, has a co-occurrence window closed yet? A pair is saved only once
   events arrive about `COOCCURRENCE_WINDOW + CO_VIEW_GAP + WATERMARK` after it
   (about 5 minutes with the defaults), because the join holds its output back
   by the gap. A burst followed by silence leaves the last window open forever.
3. Is the producer running? `docker compose ps producer`. Stop it and new
   pairs stop appearing.
4. Were events seeded after live ones? Back-dated events that arrive after
   live ones are older than the watermark and are dropped as late. The `seed`
   service runs before the producer and skips itself when results already
   exist, so this only happens if you run the seed by hand later.
5. Check `.env`. An old copy with `CO_VIEW_GAP=10 minutes` delays the first
   related product to ~17 minutes. Changing either window setting changes the
   query plan, so reset the checkpoints (see *Reset everything*).

## Diagnose: lag climbing

1. `curl localhost:8000/metrics | grep affinity_` - check
   `affinity_kafka_lag_offsets` and `affinity_state_rows` (one line per query).
2. State rows growing without bound means a stateful operator lost its
   windowing (see ADR 0001).
3. `affinity_batch_duration_ms` above the trigger interval means the batch
   cannot finish before the next one starts. In `.env`, raise
   `SHUFFLE_PARTITIONS` (up to the Spark core count), cap each batch with
   `MAX_OFFSETS_PER_TRIGGER`, or raise `TRIGGER_INTERVAL`.

## Spark UI: "Storage Memory" keeps rising

The Executors tab at http://localhost:4040/executors/ shows Storage Memory
climbing by roughly 15 MB a minute, for as long as the job runs. This is a
bookkeeping quirk of the UI in local mode, not a leak: it adds the small
broadcast blocks each micro-batch creates, but does not subtract them when
Spark removes them. The same page's API shows the real figure -
`peakMemoryMetrics.OnHeapStorageMemory` stayed at about 30 MB after an hour,
and a separate test showed the UI number rising while the real peak levelled
off. What matters for memory is the container, not that column:

```bash
docker stats affinity-spark --no-stream
```

## Reset everything

```bash
docker compose down -v
```

Deletes every volume: the Kafka log, MongoDB data, Spark checkpoints and the
monitoring history. Required after changing a query's structure, because
Spark refuses to resume from a checkpoint whose plan no longer matches.

To reset only the Spark checkpoints and keep the data:

```bash
docker compose stop spark
docker compose rm -f spark
docker volume rm streaming-product-affinity_spark_checkpoints
docker compose up -d spark
```
