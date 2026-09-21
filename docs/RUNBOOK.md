# Runbook

Every command runs from the project folder. Nothing needs to be installed on
the host except Docker.

## Verify fault tolerance

```bash
docker compose run --rm recovery
```

Sends test traffic, kills the Spark container with SIGKILL after a minute,
starts it again a minute later, and keeps sending. It then checks, per test
product, that the events Kafka acknowledged equal the events counted in
MongoDB, that no window is stored twice and that no minute is missing. Results
go to the Recovery section of `docs/BENCHMARKS.md`. Options:
`--rate 1000 --downtime 90`.

To do the same by hand and watch it:

```bash
docker compose kill spark                     # hard kill, mid-stream
sleep 60                                      # the producer keeps sending
docker compose up -d spark
docker compose logs -f spark                  # watch it resume
```

While Spark is down, http://localhost:9090/alerts shows `PipelineStale` going
from pending to firing after about three minutes (if you wait that long), and
clearing once Spark catches up.

## Measure throughput and latency

```bash
docker compose run --rm loadtest
docker compose run --rm loadtest --rates 1000 5000 --seconds 60
```

Keep the live producer running: it moves event time forward, which the pair
probes need. Writes `docs/BENCHMARKS.md` and `results/load_test.csv`. A rate
"kept up" when Spark read at least 90% of what was sent, the unread backlog
did not climb once the step had settled, and it was cleared within two
trigger intervals after the step. The tools mount `src/` and `scripts/`, so
code changes need no rebuild. Probe rows are removed at the
end; run it on a stack you can reset afterwards (`docker compose down -v`) if
you want clean dashboard history for screenshots.

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

1. Run `curl localhost:8000/stats`. Is `product_pairs` non-zero?
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

1. Run `curl localhost:8000/metrics | grep affinity_` and check
   `affinity_kafka_lag_offsets` (events not read yet, measured after each
   batch) and `affinity_state_rows` (one line per query). A lag that returns
   near zero after each batch is fine; a lag whose lowest point keeps rising
   is not.
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
Spark removes them. The same page's API shows the real figure:
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
