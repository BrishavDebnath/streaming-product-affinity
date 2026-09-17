"""
Every Spark transformation in the pipeline, as pure DataFrame -> DataFrame
functions.

They live apart from the job wiring for one reason: they can then be unit
tested against a real local SparkSession with controlled input, without Kafka,
without Mongo, without Docker. `tests/test_transforms.py` does exactly that.
Streaming logic that is only ever verified by watching a dashboard is
streaming logic nobody can refactor safely.
"""

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructType)

from src.common import config

# The wire format. Deliberately a SUPERSET of every schema version in flight,
# because Spark's from_json fills unknown fields with null rather than
# failing - which is exactly the property that lets one consumer read v1 and
# v2 at the same time.
#
#   v1  {event_id, session_id, user_id, product_id, event_type, timestamp}
#   v2  adds schema_version=2 and channel; renames event_type -> action
#
# A producer fleet never upgrades atomically: during a rollout both versions
# are on the topic simultaneously. A consumer that only understands the new
# shape drops every in-flight v1 event; one that only understands the old
# shape drops every v2. This consumer reads both, so producers and consumers
# can be deployed independently.
EVENT_SCHEMA = (
    StructType()
    .add("schema_version", IntegerType())     # absent => v1
    .add("event_id", StringType())
    .add("session_id", StringType())
    .add("user_id", IntegerType())
    .add("product_id", IntegerType())
    .add("event_type", StringType())          # v1 name
    .add("action", StringType())              # v2 name for the same thing
    .add("channel", StringType())             # v2 only
    .add("timestamp", DoubleType())
)

REQUIRED_FIELDS = ("user_id", "product_id", "event_type", "timestamp")

# Versions this consumer understands. An event claiming anything else is a
# real failure - a producer shipped ahead of its consumers - so it goes to the
# dead-letter queue loudly instead of being silently misread.
SUPPORTED_SCHEMA_VERSIONS = (1, 2)


def parse_events(raw: DataFrame) -> DataFrame:
    """
    Kafka value (bytes) -> typed columns, with an `is_valid` flag.

    Nothing is dropped here. Invalid rows are marked so the caller can route
    them to a dead-letter sink instead of silently losing them, which is what
    a bare `from_json(...).select("data.*")` does.
    """
    parsed = raw.select(
        F.col("value").cast("string").alias("raw_value"),
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data"),
    )

    flat = parsed.select("raw_value", "data.*")

    # --- schema normalisation: fold every version onto the v1 field names ---
    flat = (
        flat
        # No schema_version means the event predates versioning: that is v1.
        .withColumn("schema_version",
                    F.coalesce(F.col("schema_version"), F.lit(1)))
        # v2 calls it "action"; v1 calls it "event_type". Downstream code only
        # ever sees event_type, so adding v3 means changing this line and
        # nothing else.
        .withColumn("event_type",
                    F.coalesce(F.col("event_type"), F.col("action")))
        # v1 events have no channel. Defaulting rather than nulling keeps the
        # column usable for grouping without special-casing version.
        .withColumn("channel", F.coalesce(F.col("channel"), F.lit("unknown")))
        .drop("action")
    )

    invalid_reason = (
        F.when(~F.col("schema_version").isin(list(SUPPORTED_SCHEMA_VERSIONS)),
               F.concat(F.lit("unsupported_schema_version:"),
                        F.col("schema_version").cast("string")))
        .when(F.col("user_id").isNull(), F.lit("missing_or_invalid:user_id"))
        .when(F.col("product_id").isNull(), F.lit("missing_or_invalid:product_id"))
        .when(F.col("event_type").isNull(), F.lit("missing_or_invalid:event_type"))
        .when(F.col("timestamp").isNull(), F.lit("missing_or_invalid:timestamp"))
        .when(~F.col("event_type").isin(list(config.EVENT_TYPES)),
              F.concat(F.lit("unknown_event_type:"), F.col("event_type")))
        .otherwise(F.lit(None).cast("string"))
    )

    return (
        flat
        .withColumn("invalid_reason", invalid_reason)
        .withColumn("is_valid", F.col("invalid_reason").isNull())
        # Epoch seconds (double) -> timestamp. `.cast("timestamp")` is used
        # rather than to_timestamp() because the cast is unambiguous for a
        # numeric column across Spark versions.
        .withColumn("event_time", F.col("timestamp").cast("timestamp"))
        .withColumn("weight", event_weight(F.col("event_type")))
        # Defaults for fields a v1 producer never sends. Applied after
        # validation so an absent optional field is never a rejection reason.
        .withColumn("schema_version",
                    F.coalesce(F.col("schema_version"), F.lit(1)))
        .withColumn("channel", F.coalesce(F.col("channel"), F.lit("unknown")))
    )


def event_weight(event_type: Column) -> Column:
    """Map an event type to its affinity weight (see config.EVENT_WEIGHTS)."""
    expr = F.lit(0.0)
    for name, weight in config.EVENT_WEIGHTS.items():
        expr = F.when(event_type == F.lit(name), F.lit(float(weight))).otherwise(expr)
    return expr


def valid_events(parsed: DataFrame) -> DataFrame:
    return parsed.filter(F.col("is_valid")).drop("raw_value", "invalid_reason")


def invalid_events(parsed: DataFrame) -> DataFrame:
    return parsed.filter(~F.col("is_valid")).select(
        "raw_value", "invalid_reason",
        F.current_timestamp().alias("rejected_at"))


def with_watermark(events: DataFrame, delay: str = None) -> DataFrame:
    return events.withWatermark("event_time", delay or config.WATERMARK)


def trending(events: DataFrame,
             window_duration: str = None,
             slide: str = None) -> DataFrame:
    """
    Event counts per product per time window.

    `window()` is what makes this state bounded: event_time is part of the
    grouping key, so the watermark can evict closed windows. Grouping by
    product_id alone would retain every product forever.
    """
    window_duration = window_duration or config.TRENDING_WINDOW
    slide = slide if slide is not None else config.TRENDING_SLIDE
    bucket = (F.window(F.col("event_time"), window_duration, slide)
              if slide else F.window(F.col("event_time"), window_duration))

    return (
        events.groupBy(bucket, F.col("product_id"))
        .agg(F.count("*").alias("event_count"),
             F.sum("weight").alias("score"),
             F.approx_count_distinct("user_id").alias("unique_users"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("product_id"),
            F.col("event_count"),
            F.round(F.col("score"), 3).alias("score"),
            F.col("unique_users"),
        )
    )


def co_occurrence(events: DataFrame,
                  gap: str = None,
                  window_duration: str = None) -> DataFrame:
    """
    "Users who interacted with A also interacted with B."

    A stream-stream self-join on user_id, constrained so the two events fall
    within `gap` of each other. The time constraint plus the watermark is what
    bounds the join state — without it Spark would have to keep every event
    forever in case a future event joined to it.

    Ordering the pair by product id (a.product_id < b.product_id) means (A,B)
    and (B,A) are the same row; the result is mirrored afterwards so a lookup
    on either side finds it.
    """
    gap = gap or config.CO_VIEW_GAP
    window_duration = window_duration or config.COOCCURRENCE_WINDOW

    # Events with no session id (dashboard clicks) still get one, derived from
    # the user, so a hand-clicked event is never silently dropped.
    events = events.withColumn(
        "join_session",
        F.coalesce(F.col("session_id"),
                   F.concat(F.lit("u"), F.col("user_id").cast("string"))))

    left = events.select(
        F.col("join_session").alias("l_session"),
        F.col("user_id").alias("l_user"),
        F.col("product_id").alias("l_product"),
        F.col("event_time").alias("l_time"),
        F.col("weight").alias("l_weight"),
    )
    right = events.select(
        F.col("join_session").alias("r_session"),
        F.col("user_id").alias("r_user"),
        F.col("product_id").alias("r_product"),
        F.col("event_time").alias("r_time"),
        F.col("weight").alias("r_weight"),
    )

    joined = left.join(
        right,
        # Joined on SESSION, not just user - see producer.make_session.
        (F.col("l_session") == F.col("r_session"))
        & (F.col("l_product") < F.col("r_product"))          # de-duplicate pairs
        & (F.col("r_time") >= F.col("l_time") - F.expr(f"INTERVAL {gap}"))
        & (F.col("r_time") <= F.col("l_time") + F.expr(f"INTERVAL {gap}")),
        how="inner",
    )

    # The window is bucketed on `l_time` rather than on a derived column such
    # as least(l_time, r_time). A derived column loses its event-time
    # attribution, and Spark then rejects the windowed aggregation below with
    # STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION; re-declaring the watermark
    # is not an option either, since Spark 4 disallows redefining it. `l_time`
    # is the join's left event time and still carries the watermark, so the
    # aggregation stays legal and the state stays bounded.
    pairs = (
        joined
        # Geometric mean keeps a strong+weak pair below a strong+strong pair.
        .withColumn("pair_weight", F.sqrt(F.col("l_weight") * F.col("r_weight")))
        .groupBy(F.window(F.col("l_time"), window_duration),
                 F.col("l_product"), F.col("r_product"))
        .agg(F.count("*").alias("pair_count"),
             F.sum("pair_weight").alias("affinity"),
             F.approx_count_distinct("l_user").alias("unique_users"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("l_product").alias("product_id"),
            F.col("r_product").alias("related_product_id"),
            F.col("pair_count"),
            F.round(F.col("affinity"), 3).alias("affinity"),
            F.col("unique_users"),
        )
    )
    return pairs


def mirror_pairs(pairs: DataFrame) -> DataFrame:
    """
    Emit both (A,B) and (B,A) so either product can be looked up.

    Apply this to the BATCH DataFrame inside foreachBatch, never inside the
    streaming plan. Unioning a streaming aggregation with a projection of
    itself creates two references to one stateful operator, and only one
    branch reliably emits - verified: the streaming query produced (9001,9003)
    but never its mirror. In foreachBatch the input is an ordinary batch
    DataFrame, so the union behaves normally.
    """
    flipped = pairs.select(
        "window_start", "window_end",
        F.col("related_product_id").alias("product_id"),
        F.col("product_id").alias("related_product_id"),
        "pair_count", "affinity", "unique_users",
    )
    return pairs.unionByName(flipped)
