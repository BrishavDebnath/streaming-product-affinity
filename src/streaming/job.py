"""
The Spark Structured Streaming job.

Wiring only — all logic lives in `transforms.py` so it can be unit tested.

Three queries run concurrently:
  1. trending          windowed event counts per product
  2. product_pairs     stream-stream self-join -> co-viewed product pairs
  3. dead_letter       events that failed validation, with the reason

Run:
    spark-submit --master local[*] src/streaming/job.py
"""

import logging
import sys
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient, UpdateOne
from pyspark.sql import SparkSession

from src.common import config, kafka_io, mongo
from src.streaming import transforms as T

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
)
log = logging.getLogger("streaming.job")
# py4j logs every Python<->JVM callback at INFO (several per micro-batch):
# three quarters of the Spark container's log was "Received command c on
# object id ...". Only its warnings are useful.
logging.getLogger("py4j").setLevel(logging.WARNING)
# kafka-python (used to read the broker's latest offsets) logs every
# connection step at INFO.
logging.getLogger("kafka").setLevel(logging.WARNING)

# MongoDB clients come from src.common.mongo: one per Python process, reused
# across partitions and micro-batches. The cache must NOT live in this file -
# see that module for why.


def mongo_upsert(collection_name: str, key_fields):
    """
    foreachBatch sink that UPSERTS on a natural key.

    The original pipeline used `.mode("append")` with `outputMode("update")`,
    so every micro-batch inserted a NEW document for a window that already
    existed. `/trending` then returned the same product many times with stale
    counts. Upserting on the natural key makes the write idempotent: replaying
    a batch after a failure converges to the same state instead of duplicating
    it.
    """
    def _write(batch_df, epoch_id):
        def _write_partition(rows):
            """
            Runs on the EXECUTOR, one connection per partition.

            The previous version called batch_df.collect(), pulling the whole
            micro-batch into the DRIVER before writing. Fine for a 12-product
            catalogue, fatal for a large one: the driver heap becomes the
            ceiling on batch size. foreachPartition keeps the data distributed
            and writes in parallel.
            """
            coll = mongo.client(config.MONGO_URI)[config.MONGO_DB][collection_name]
            buffer = []

            def flush():
                # Stamped when the rows are actually written. A time taken
                # when foreachBatch starts is BEFORE Spark computes the batch
                # (it is lazy), so latency and freshness read too low.
                now = datetime.now(timezone.utc)
                coll.bulk_write(
                    [UpdateOne(key, {"$set": {**doc, "_updated_at": now}},
                               upsert=True) for key, doc in buffer],
                    ordered=False)
                buffer.clear()

            for row in rows:
                doc = row.asDict()
                buffer.append(({f: doc[f] for f in key_fields}, doc))
                if len(buffer) >= 1000:      # bound memory per partition
                    flush()
            if buffer:
                flush()

        batch_df.foreachPartition(_write_partition)
        log.info("epoch=%s collection=%s written", epoch_id, collection_name)
    return _write


def mongo_append(collection_name: str):
    """Plain insert sink, used for the dead-letter queue where every row counts."""
    def _write(batch_df, epoch_id):
        def _write_partition(rows):
            coll = mongo.client(config.MONGO_URI)[config.MONGO_DB][collection_name]
            buffer = []

            def flush():
                # The TTL index is on _updated_at; without this field
                # dead-letter rows never expired. Stamped at write time.
                now = datetime.now(timezone.utc)
                coll.insert_many([{**doc, "_updated_at": now} for doc in buffer],
                                 ordered=False)
                buffer.clear()

            for row in rows:
                buffer.append(row.asDict())
                if len(buffer) >= 1000:
                    flush()
            if buffer:
                flush()

        batch_df.foreachPartition(_write_partition)
        log.debug("epoch=%s dead-letter batch written", epoch_id)
    return _write


def mirror_then_upsert(collection_name: str, key_fields):
    """Mirror pairs inside the batch, then upsert. See transforms.mirror_pairs."""
    upsert = mongo_upsert(collection_name, key_fields)

    def _write(batch_df, epoch_id):
        upsert(T.mirror_pairs(batch_df), epoch_id)
    return _write


def ensure_indexes():
    """Without these, every API read is a collection scan."""
    client = MongoClient(config.MONGO_URI)
    try:
        db = client[config.MONGO_DB]
        db[config.COLL_TRENDING].create_index(
            [("window_start", -1), ("score", -1)])
        db[config.COLL_TRENDING].create_index(
            [("window_start", 1), ("window_end", 1), ("product_id", 1)],
            unique=True)
        db[config.COLL_PAIRS].create_index(
            [("product_id", 1), ("affinity", -1)])
        db[config.COLL_TRENDING].create_index([("_updated_at", -1)])

        # TTL indexes. Without these, `trending` and `product_pairs` grow
        # forever: the pipeline writes a row per window per product, every
        # window, indefinitely. MongoDB deletes documents whose _updated_at is
        # older than the retention period automatically.
        retention = config.RETENTION_HOURS * 3600
        for coll in (config.COLL_TRENDING, config.COLL_PAIRS,
                     config.COLL_DLQ):
            try:
                db[coll].create_index([("_updated_at", 1)],
                                      expireAfterSeconds=retention,
                                      name="ttl__updated_at")
            except Exception as exc:                      # noqa: BLE001
                log.warning("TTL index on %s not created: %s", coll, exc)
        db[config.COLL_PAIRS].create_index(
            [("window_start", 1), ("window_end", 1),
             ("product_id", 1), ("related_product_id", 1)], unique=True)
        log.info("MongoDB indexes ensured on %s", config.MONGO_DB)
    finally:
        client.close()


def _to_float(value):
    """Spark reports source metrics as strings; None if absent or unparsable."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class ProgressRecorder:
    """
    StreamingQueryListener that writes each micro-batch's progress to Mongo.

    Kafka consumer lag, input rate and state-store size live inside the Spark
    driver; the API is a separate process and cannot see them. Persisting them
    each batch is what makes /metrics report real pipeline health instead of
    just HTTP counters.
    """

    def __init__(self, uri, database, collection="pipeline_metrics"):
        self._uri, self._db, self._coll = uri, database, collection

    def _write(self, doc):
        try:
            coll = mongo.client(self._uri)[self._db][self._coll]
            coll.insert_one(doc)
            # Keep only recent history; this collection is monitoring data,
            # not a system of record.
            coll.delete_many({"recorded_at": {"$lt": datetime.now(timezone.utc)
                                              - timedelta(hours=2)}})
        except Exception as exc:                          # noqa: BLE001
            log.debug("progress write skipped: %s", exc)

    def _lag(self, sources):
        """
        Events each source still has to read, measured now, after the batch.

        Spark's maxOffsetsBehindLatest compares against the offsets seen when
        the batch was PLANNED, so it reads 0 whenever batches are not
        size-capped (see kafka_io.latest_offsets). It is only used when
        MAX_OFFSETS_PER_TRIGGER caps the batches; otherwise an unreadable
        broker gives None ("unknown"), never a reassuring 0.
        """
        latest = kafka_io.latest_offsets(config.TOPIC_EVENTS,
                                         config.KAFKA_BOOTSTRAP)
        lags = []
        for s in sources:
            lag = kafka_io.offsets_behind(getattr(s, "endOffset", None),
                                          latest, config.TOPIC_EVENTS)
            if lag is None and config.MAX_OFFSETS_PER_TRIGGER > 0:
                lag = _to_float((getattr(s, "metrics", None) or {})
                                .get("maxOffsetsBehindLatest"))
            lags.append(lag)
        return lags

    def onQueryProgress(self, event):                     # noqa: N802
        p = event.progress
        sources = getattr(p, "sources", []) or []
        doc = {
            "recorded_at": datetime.now(timezone.utc),
            "query": p.name,
            "batch_id": p.batchId,
            "input_rows": getattr(p, "numInputRows", None),
            "input_rows_per_second": getattr(p, "inputRowsPerSecond", None),
            "processed_rows_per_second": getattr(p, "processedRowsPerSecond", None),
            "batch_duration_ms": getattr(p, "batchDuration", None),
            # PySpark passes StateOperatorProgress / SourceProgress OBJECTS,
            # not dicts: calling .get() on them raised AttributeError on every
            # batch, so nothing was ever recorded.
            "state_rows": [getattr(so, "numRowsTotal", None)
                           for so in (getattr(p, "stateOperators", []) or [])],
            "kafka_lag": self._lag(sources),
        }
        self._write(doc)

    def onQueryStarted(self, event):                      # noqa: N802
        log.info("query started: %s", event.name)

    def onQueryTerminated(self, event):                   # noqa: N802
        log.warning("query terminated: %s", event.id)


def make_listener(uri, database):
    """
    Build the StreamingQueryListener that feeds /metrics.

    ProgressRecorder must come FIRST in the bases. StreamingQueryListener
    declares these callbacks abstract; listed first, its abstract versions win
    the method lookup and Python refuses to instantiate the class - which the
    job used to swallow, so /metrics silently never had pipeline internals.
    """
    from pyspark.sql.streaming import StreamingQueryListener

    class _Listener(ProgressRecorder, StreamingQueryListener):
        pass

    return _Listener(uri, database)


# Spark 4.1 logs a WARN with a full stack trace several times per micro-batch
# ("Error trying to extract state constraint ... Cannot evaluate expression:
# l_product") because StreamingJoinHelper also inspects the non-time
# comparison `l_product < r_product` in the join condition. It is noise, not a
# fault: measured over 20 minutes of events the join state stays flat (~600
# rows) and pairs are emitted on time. Only that one logger is quietened.
NOISY_LOGGERS = ("org.apache.spark.sql.catalyst.analysis.StreamingJoinHelper",)


def quiet_known_noise(spark: SparkSession) -> None:
    try:
        jvm = spark.sparkContext._jvm
        assert jvm is not None                # only None before the JVM starts
        level = jvm.org.apache.logging.log4j.Level.ERROR
        for name in NOISY_LOGGERS:
            jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(name, level)
    except Exception as exc:                              # noqa: BLE001
        log.debug("could not adjust Spark log levels: %s", exc)


def state_store_provider() -> str:
    return config.STATE_STORE_PROVIDERS[config.STATE_STORE]


def build_spark() -> SparkSession:
    builder = (SparkSession.builder
               .appName("streaming-product-affinity")
               .config("spark.sql.shuffle.partitions", str(config.SHUFFLE_PARTITIONS))
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.streaming.stateStore.providerClass",
                       state_store_provider()))
    if config.STATE_STORE == "rocksdb":
        # Upload only the changes each batch instead of a full snapshot.
        builder = builder.config(
            "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled",
            "true")
    return builder.getOrCreate()


def main() -> int:
    ensure_indexes()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    quiet_known_noise(spark)
    log.info("Kafka=%s topic=%s mongo=%s state_store=%s shuffle_partitions=%s",
             config.KAFKA_BOOTSTRAP, config.TOPIC_EVENTS, config.MONGO_DB,
             config.STATE_STORE, config.SHUFFLE_PARTITIONS)

    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
              .option("subscribe", config.TOPIC_EVENTS)
              .option("startingOffsets", config.STARTING_OFFSETS)
              .option("failOnDataLoss", "false"))
    if config.MAX_OFFSETS_PER_TRIGGER > 0:
        reader = reader.option("maxOffsetsPerTrigger", config.MAX_OFFSETS_PER_TRIGGER)
    raw = reader.load()

    parsed = T.parse_events(raw)
    events = T.with_watermark(T.valid_events(parsed))

    trending_df = T.trending(events)
    pairs_df = T.co_occurrence(events)
    rejects_df = T.invalid_events(parsed)

    trigger = {"processingTime": config.TRIGGER_INTERVAL}

    queries = [
        (trending_df.writeStream
         .foreachBatch(mongo_upsert(
             config.COLL_TRENDING,
             ["window_start", "window_end", "product_id"]))
         .outputMode("update")
         .option("checkpointLocation", f"{config.CHECKPOINT_ROOT}/trending")
         .trigger(**trigger)
         .queryName("trending")
         .start()),

        (pairs_df.writeStream
         .foreachBatch(mirror_then_upsert(
             config.COLL_PAIRS,
             ["window_start", "window_end", "product_id", "related_product_id"]))
         # Append: a stream-stream join followed by a windowed aggregation
         # emits a window only once the watermark has passed its end.
         .outputMode("append")
         .option("checkpointLocation", f"{config.CHECKPOINT_ROOT}/product_pairs")
         .trigger(**trigger)
         .queryName("product_pairs")
         .start()),

        (rejects_df.writeStream
         .foreachBatch(mongo_append(config.COLL_DLQ))
         .outputMode("append")
         .option("checkpointLocation", f"{config.CHECKPOINT_ROOT}/dead_letter")
         .trigger(**trigger)
         .queryName("dead_letter")
         .start()),
    ]

    for q in queries:
        log.info("started query: %s", q.name)

    # Best-effort: the listener API differs across Spark versions, and losing
    # monitoring must never take the pipeline down.
    try:
        listener = make_listener(config.MONGO_URI, config.MONGO_DB)
        spark.streams.addListener(listener)
        log.info("progress recorder attached")
    except Exception as exc:                              # noqa: BLE001
        log.warning("progress recorder unavailable (%s); "
                    "/metrics will omit pipeline internals", exc)
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
