"""
Unit tests for the streaming transforms, against a real local SparkSession.

No Kafka, no Mongo, no Docker. Batch tests cover the pure logic; the streaming
test drives a real Structured Streaming query through a file source so the
stream-stream join and the watermark are genuinely exercised.

    python tests/test_transforms.py
"""

import ast
import json
import math
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, functions as F

from src.common import catalog, config, scoring
from src.streaming import transforms as T

FAILURES = []


def check(name, ok, detail=""):
    print(("PASS  " if ok else "FAIL  ") + name + ("" if ok else f": {detail}"))
    if not ok:
        FAILURES.append(name)


def spark_session():
    return (SparkSession.builder
            .appName("transform-tests")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            # Applied while the SparkContext starts, so the ~40 INFO start-up
            # lines are not printed before setLogLevel() can run.
            .config("spark.log.level", "ERROR")
            .getOrCreate())


def as_kafka_rows(spark, events):
    """Mimic what the Kafka source hands us: a binary `value` column."""
    payloads = [(json.dumps(e).encode("utf-8"),) for e in events]
    return spark.createDataFrame(payloads, "value binary")


def event(user, product, etype="view", t=1_700_000_000.0, session=None):
    return {"event_id": f"{user}-{product}-{t}",
            "session_id": session if session is not None else f"s{user}",
            "user_id": user, "product_id": product,
            "event_type": etype, "timestamp": t}


# ----------------------------------------------------------------- parsing
def test_parsing(spark):
    rows = [
        event(1001, 9001, "view"),
        event(1001, 9003, "add_to_cart"),
        {"user_id": 1002, "product_id": 9001,
         "event_type": "teleport", "timestamp": 1_700_000_000.0},   # bad type
        {"user_id": None, "product_id": 9001,
         "event_type": "view", "timestamp": 1_700_000_000.0},       # null user
    ]
    parsed = T.parse_events(as_kafka_rows(spark, rows))
    valid = T.valid_events(parsed).collect()
    invalid = T.invalid_events(parsed).collect()

    check("parser keeps well-formed events", len(valid) == 2, f"got {len(valid)}")
    check("parser rejects unknown event types and nulls",
          len(invalid) == 2, f"got {len(invalid)}")
    reasons = sorted(r["invalid_reason"] for r in invalid)
    check("rejection reason is recorded",
          reasons == ["missing_or_invalid:user_id", "unknown_event_type:teleport"],
          str(reasons))
    check("malformed rows keep their raw payload for replay",
          all(r["raw_value"] for r in invalid))

    weights = {r["event_type"]: r["weight"] for r in valid}
    check("event weights applied",
          weights["view"] == config.EVENT_WEIGHTS["view"]
          and weights["add_to_cart"] == config.EVENT_WEIGHTS["add_to_cart"],
          str(weights))

    ts = T.valid_events(parsed).select("event_time").collect()[0]["event_time"]
    check("epoch double casts to a real timestamp", ts is not None and ts.year == 2023,
          str(ts))


def test_garbage_input_does_not_crash(spark):
    payloads = [(b"not json at all",), (b"{",), (b"{}",)]
    raw = spark.createDataFrame(payloads, "value binary")
    parsed = T.parse_events(raw)
    valid = T.valid_events(parsed).count()
    invalid = T.invalid_events(parsed).count()
    check("garbage payloads all route to the DLQ, none crash the job",
          valid == 0 and invalid == 3, f"valid={valid} invalid={invalid}")


# ---------------------------------------------------------------- trending
def test_trending(spark):
    base = 1_700_000_000.0
    rows = ([event(1001, 9001, "view", base + i) for i in range(5)]
            + [event(1002, 9001, "purchase", base + 6)]
            + [event(1003, 9002, "view", base + 7)])
    events = T.valid_events(T.parse_events(as_kafka_rows(spark, rows)))
    out = {r["product_id"]: r for r in T.trending(events, "10 minutes").collect()}

    check("trending counts events per product",
          out[9001]["event_count"] == 6 and out[9002]["event_count"] == 1,
          str({k: v["event_count"] for k, v in out.items()}))
    expected = 5 * config.EVENT_WEIGHTS["view"] + config.EVENT_WEIGHTS["purchase"]
    check("trending score is weighted by event type",
          abs(out[9001]["score"] - expected) < 1e-6,
          f"{out[9001]['score']} vs {expected}")
    check("trending counts distinct users",
          out[9001]["unique_users"] == 2, str(out[9001]["unique_users"]))
    check("trending emits window bounds",
          out[9001]["window_start"] is not None and out[9001]["window_end"] is not None)


def test_trending_separates_windows(spark):
    base = 1_700_000_000.0
    rows = [event(1001, 9001, "view", base),
            event(1001, 9001, "view", base + 3600)]     # an hour later
    events = T.valid_events(T.parse_events(as_kafka_rows(spark, rows)))
    out = T.trending(events, "1 minute").collect()
    check("events an hour apart land in different windows", len(out) == 2,
          f"got {len(out)} rows")


# ----------------------------------------------------------- co-occurrence
def test_co_occurrence_batch(spark):
    base = 1_700_000_000.0
    rows = [
        # user 1001: laptop + sleeve, close together
        event(1001, 9001, "view", base),
        event(1001, 9003, "add_to_cart", base + 30),
        # user 1002: same pair
        event(1002, 9001, "view", base + 60),
        event(1002, 9003, "view", base + 90),
        # user 1003: laptop + shoes, far apart -> outside the gap
        event(1003, 9001, "view", base),
        event(1003, 9011, "view", base + 3600),
    ]
    events = T.valid_events(T.parse_events(as_kafka_rows(spark, rows)))
    canonical = T.co_occurrence(events, gap="10 minutes", window_duration="1 hour")
    pairs = T.mirror_pairs(canonical).collect()
    lookup = {(r["product_id"], r["related_product_id"]): r for r in pairs}

    check("co-occurrence emits each pair once in canonical order",
          all(r["product_id"] < r["related_product_id"] for r in canonical.collect()))

    check("co-occurrence finds the co-viewed pair",
          (9001, 9003) in lookup, str(sorted(lookup)))
    check("pair is mirrored so either product can be looked up",
          (9003, 9001) in lookup, str(sorted(lookup)))
    check("pair count aggregates across users",
          lookup[(9001, 9003)]["pair_count"] == 2,
          str(lookup[(9001, 9003)]["pair_count"]))
    check("events outside the gap do not form a pair",
          (9001, 9011) not in lookup and (9011, 9001) not in lookup)
    check("a product is never paired with itself",
          all(r["product_id"] != r["related_product_id"] for r in pairs))
    check("affinity is weighted, not a raw count",
          lookup[(9001, 9003)]["affinity"] > lookup[(9001, 9003)]["pair_count"],
          str(lookup[(9001, 9003)]))


def test_v1_and_v2_events_both_parse(spark):
    """
    A producer fleet is never upgraded atomically. Both versions must parse.
    """
    base = 1_700_000_000.0
    v1 = {"event_id": "a", "user_id": 1001, "product_id": 9001,
          "event_type": "view", "timestamp": base}
    v2 = {"schema_version": 2, "event_id": "b", "session_id": "s1",
          "channel": "android", "user_id": 1001, "product_id": 9003,
          "event_type": "view", "timestamp": base + 1}
    future = {"schema_version": 99, "event_id": "c", "user_id": 1001,
              "product_id": 9001, "event_type": "view", "timestamp": base}

    parsed = T.parse_events(as_kafka_rows(spark, [v1, v2, future]))
    valid = {r["event_id"]: r for r in T.valid_events(parsed).collect()}
    invalid = T.invalid_events(parsed).collect()

    check("v1 event (no schema_version) parses", "a" in valid, str(list(valid)))
    check("v2 event parses", "b" in valid, str(list(valid)))
    check("v1 defaults schema_version to 1",
          valid["a"]["schema_version"] == 1 if "a" in valid else False)
    check("v1 defaults channel to 'unknown'",
          valid["a"]["channel"] == "unknown" if "a" in valid else False)
    check("v2 keeps its channel",
          valid["b"]["channel"] == "android" if "b" in valid else False)
    check("a future schema version is rejected, not mis-parsed",
          len(invalid) == 1
          and invalid[0]["invalid_reason"].startswith("unsupported_schema_version"),
          str([r["invalid_reason"] for r in invalid]))
    check("the rejected row keeps its payload for replay",
          bool(invalid[0]["raw_value"]) if invalid else False)


def test_v1_events_still_produce_pairs(spark):
    """v1 has no session_id; it must fall back, not vanish."""
    base = 1_700_000_000.0
    rows = [{"event_id": f"v1-{i}", "user_id": 1001, "product_id": pid,
             "event_type": "view", "timestamp": base + i}
            for i, pid in enumerate((9001, 9003))]
    events = T.valid_events(T.parse_events(as_kafka_rows(spark, rows)))
    found = {(r["product_id"], r["related_product_id"])
             for r in T.co_occurrence(events, gap="10 minutes",
                                      window_duration="1 hour").collect()}
    check("v1 events still form co-occurrence pairs",
          (9001, 9003) in found, str(found))


def test_co_occurrence_requires_the_same_session(spark):
    """
    Guards the quadratic blowup: keying the join on user_id alone paired every
    event a continuously-active user made with every other one - ~73x too many
    pairs, and a ranking that was really just popularity.
    """
    base = 1_700_000_000.0
    rows = [
        event(1001, 9001, "view", base, session="visit-A"),
        event(1001, 9011, "view", base + 5, session="visit-B"),   # other visit
        event(1002, 9005, "view", base, session="visit-C"),
        event(1002, 9007, "view", base + 3, session="visit-C"),   # one visit
    ]
    events = T.valid_events(T.parse_events(as_kafka_rows(spark, rows)))
    found = {(r["product_id"], r["related_product_id"])
             for r in T.co_occurrence(events, gap="10 minutes",
                                      window_duration="1 hour").collect()}
    check("products from two different visits are NOT paired",
          (9001, 9011) not in found, str(found))
    check("products from one visit ARE paired", (9005, 9007) in found, str(found))


def test_events_without_a_session_still_pair(spark):
    """Dashboard clicks carry no session_id; they must not vanish."""
    base = 1_700_000_000.0
    rows = []
    for pid, off in ((9001, 0.0), (9003, 2.0)):
        r = event(1001, pid, "view", base + off)
        r.pop("session_id")
        rows.append(r)
    events = T.valid_events(T.parse_events(as_kafka_rows(spark, rows)))
    found = {(r["product_id"], r["related_product_id"])
             for r in T.co_occurrence(events, gap="10 minutes",
                                      window_duration="1 hour").collect()}
    check("sessionless events fall back to pairing by user",
          (9001, 9003) in found, str(found))


def test_co_occurrence_ignores_cross_user(spark):
    base = 1_700_000_000.0
    rows = [event(1001, 9001, "view", base), event(1002, 9003, "view", base + 5)]
    events = T.valid_events(T.parse_events(as_kafka_rows(spark, rows)))
    pairs = T.co_occurrence(events, gap="10 minutes", window_duration="1 hour").collect()
    check("two different users browsing at once do not create a pair",
          len(pairs) == 0, f"got {len(pairs)} pairs")


# ------------------------------------------------- real streaming exercise
def test_streaming_end_to_end(spark):
    """
    Drive a genuine Structured Streaming query: file source -> parse ->
    watermark -> stream-stream self-join -> windowed aggregation -> memory
    sink. This is the only test that proves the join is legal in streaming
    mode; a stream-stream join without a time constraint is rejected at plan
    time, and a windowed aggregation on a derived time column is too.

    Two input files are used deliberately. In append mode a window only emits
    once the watermark has passed its end, so the second batch exists purely
    to advance event time far enough to close the first batch's window.
    """
    tmp = tempfile.mkdtemp(prefix="stream_in_")
    ckpt = tempfile.mkdtemp(prefix="stream_ck_")
    try:
        base = 1_700_000_000.0
        batch1 = [event(1001, 9001, "view", base),
                  event(1001, 9003, "add_to_cart", base + 2),
                  event(1002, 9001, "view", base + 4),
                  event(1002, 9003, "view", base + 6)]
        # Later batches exist only to advance event time so batch 1's window
        # closes. Three are needed, not two: on Spark 3.5 a query with chained
        # stateful operators (stream-stream join -> windowed aggregation) uses
        # a global watermark that lags one micro-batch behind, so two files
        # emit nothing. Spark 4 emits on two. Three works on both.
        batch2 = [event(1009, 9012, "view", base + 600),
                  event(1009, 9011, "view", base + 602)]
        batch3 = [event(1010, 9012, "view", base + 1800),
                  event(1010, 9011, "view", base + 1802)]

        # The file source orders by MODIFICATION TIME, not filename, so the
        # files are written with a pause between them. Without this the later
        # batches can be read first, advancing the watermark past batch 1 and
        # causing its events to be dropped as late.
        for name, rows in (("a_batch1.json", batch1),
                           ("b_batch2.json", batch2),
                           ("c_batch3.json", batch3)):
            with open(os.path.join(tmp, name), "w", encoding="utf-8") as f:
                for r in rows:
                    f.write(json.dumps(r) + "\n")
            time.sleep(1.1)

        raw = (spark.readStream
               .schema("value string")
               .option("maxFilesPerTrigger", 1)
               .text(tmp)
               .select(F.col("value").cast("binary").alias("value")))

        parsed = T.parse_events(raw)
        events = T.with_watermark(T.valid_events(parsed), "1 seconds")
        pairs = T.co_occurrence(events, gap="10 seconds",
                                window_duration="10 seconds")

        query = (pairs.writeStream
                 .format("memory").queryName("pairs_out")
                 .outputMode("append")
                 .option("checkpointLocation", ckpt)
                 .start())
        # Poll until the pair we care about appears, not merely until SOME
        # row appears: on Spark 3.5 a later batch's window can close first.
        deadline = time.time() + 150
        got = []
        while time.time() < deadline:
            time.sleep(3)
            got = spark.sql("SELECT * FROM pairs_out").collect()
            if any({r["product_id"], r["related_product_id"]} == {9001, 9003}
                   for r in got):
                break
        # Let the running micro-batch finish before stopping. Stopping mid-batch
        # aborts it, and on Spark 4.1 its tasks can still be writing checksum
        # files when the checkpoint folder is removed below - harmless, but it
        # prints ERROR stack traces into the test log.
        query.processAllAvailable()
        progress = query.lastProgress
        query.stop()

        check("streaming stream-stream join runs and emits pairs",
              len(got) > 0, "no rows emitted within 120 s")
        if got:
            found = {(r["product_id"], r["related_product_id"]) for r in got}
            check("streaming pipeline finds the same pair as the batch test",
                  (9001, 9003) in found, str(found))
            check("streaming plan emits canonical order only (mirror happens "
                  "in foreachBatch)",
                  all(a < b for a, b in found), str(found))
            counts = {(r["product_id"], r["related_product_id"]): r["pair_count"]
                      for r in got}
            check("streaming pair_count aggregates both users",
                  counts.get((9001, 9003)) == 2, str(counts))
        if progress and progress.get("stateOperators"):
            rows_total = [so["numRowsTotal"] for so in progress["stateOperators"]]
            check("streaming state is bounded (windows close and evict)",
                  all(n < 1000 for n in rows_total), str(rows_total))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        shutil.rmtree(ckpt, ignore_errors=True)



def test_join_state_is_evicted_over_time(spark):
    """
    The time-bounded self-join must forget old events. Ten minutes of steady
    sessions are fed one minute at a time; once the watermark and co-view gap
    have passed, the join state has to stop growing. On Spark 4.1 this was
    measured flat at ~600 rows from minute 6 onward.
    """
    tmp = tempfile.mkdtemp(prefix="evict_in_")
    ckpt = tempfile.mkdtemp(prefix="evict_ck_")
    try:
        raw = (spark.readStream.schema("value string")
               .option("maxFilesPerTrigger", 1).text(tmp)
               .select(F.col("value").cast("binary").alias("value")))
        events = T.with_watermark(T.valid_events(T.parse_events(raw)), "2 minutes")
        pairs = T.co_occurrence(events, gap="2 minutes", window_duration="1 minute")
        query = (pairs.writeStream.format("memory").queryName("evict_out")
                 .outputMode("append").option("checkpointLocation", ckpt)
                 .start())
        join_rows = {}
        base = 1_700_000_000.0
        for minute in range(10):
            with open(os.path.join(tmp, f"m{minute:02d}.json"), "w",
                      encoding="utf-8") as f:
                for s in range(20):
                    for k, product in enumerate((9001, 9002, 9003)):
                        f.write(json.dumps(event(
                            1000 + s, product, "view",
                            base + minute * 60 + s + k,
                            session=f"m{minute}-s{s}")) + "\n")
            query.processAllAvailable()
            progress = query.lastProgress or {}
            for op in progress.get("stateOperators", []):
                if op.get("operatorName") == "symmetricHashJoin":
                    join_rows[minute] = op["numRowsTotal"]
        emitted = spark.sql("SELECT COUNT(*) AS c FROM evict_out").collect()[0]["c"]
        query.stop()

        check("time-bounded join emits pairs while evicting",
              emitted > 0, f"emitted={emitted}")
        early, late = join_rows.get(6), join_rows.get(9)
        check("join state stops growing once the gap and watermark pass",
              early is not None and late is not None and late <= early * 1.2,
              str(join_rows))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        shutil.rmtree(ckpt, ignore_errors=True)


# ---------------------------------------------------------------- catalog
def test_catalog_consistency():
    catalog.validate()
    ids = catalog.product_ids()
    check("catalogue ids are unique", len(ids) == len(set(ids)))
    check("catalogue lookup works", catalog.name_of(9001).startswith("Apple"))
    check("unknown product degrades gracefully",
          "Unknown" in catalog.name_of(1), catalog.name_of(1))
    enriched = catalog.enrich([{"product_id": 9001, "event_count": 3}])
    check("enrich attaches catalogue fields",
          enriched[0]["name"] and enriched[0]["price"], str(enriched))
    check("producer and UI share one product space",
          set(catalog.product_ids()) == {p["id"] for p in catalog.PRODUCTS})


def test_dashboard_dot_generation():
    """The affinity graph is a DOT string; malformed DOT renders as nothing."""
    # Pull build_dot out of the source, so the test needs no Streamlit.
    src = open(os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "src", "ui", "dashboard.py"),
        encoding="utf-8").read()
    start = src.index("def build_dot")
    end = src.index("st.title(")
    ns = {"CATEGORY_COLOUR": {"laptop": "#2563eb", "laptop-acc": "#60a5fa",
                              "unknown": "#9ca3af"}}
    exec(src[start:end].split("\n\n\n")[0], ns)          # noqa: S102
    build_dot = ns["build_dot"]

    graph = {"nodes": [{"id": 9001, "name": 'Apple MacBook Air M3',
                        "category": "laptop", "degree": 1},
                       {"id": 9003, "name": 'Laptop Sleeve 13"',
                        "category": "laptop-acc", "degree": 1}],
             "edges": [{"source": 9001, "target": 9003,
                        "affinity": 42.6, "pair_count": 18}]}
    dot = build_dot(graph)
    check("DOT is well formed",
          dot.startswith("graph G {") and dot.rstrip().endswith("}"))
    check("DOT contains the edge", "n9001 -- n9003" in dot)
    check("double quotes in product names are escaped",
          'Laptop Sleeve 13\'' in dot, dot)

    from src.common import catalog as cat
    mixed = {"nodes": [{"id": 9001, "name": "A", "category": "laptop"},
                       {"id": 9003, "name": "B", "category": "laptop-acc"},
                       {"id": 9011, "name": "C", "category": "footwear"}],
             "edges": [{"source": 9001, "target": 9003, "affinity": 4.0,
                        "pair_count": 9},
                       {"source": 9001, "target": 9011, "affinity": 1.0,
                        "pair_count": 3}]}
    styled = build_dot(mixed, cat.categories_related)
    check("related categories get a solid line, others a dashed one",
          "n9001 -- n9003 [penwidth=6.00 color=\"#94a3b8\" style=solid" in styled
          and "n9001 -- n9011" in styled
          and styled.split("n9001 -- n9011")[1].split("\n")[0].count("dashed") == 1,
          styled)
    check("category links are symmetric",
          cat.categories_related("laptop-acc", "laptop")
          and cat.categories_related("audio", "phone")
          and not cat.categories_related("footwear", "phone"))

    single = build_dot({"nodes": [{"id": 1, "name": "X", "category": "unknown",
                                   "degree": 0}], "edges": []})
    check("empty edge list does not divide by zero", "graph G {" in single)


def test_producer_uses_a_single_kafka_connection():
    """The malformed branch used to build a second producer per event."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src = open(os.path.join(root, "src", "producer", "producer.py"),
               encoding="utf-8").read()
    check("producer constructs exactly one KafkaProducer",
          src.count("KafkaProducer(") == 1, f"found {src.count('KafkaProducer(')}")
    in_loop = False
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.While):
            for inner in ast.walk(node):
                if (isinstance(inner, ast.Call)
                        and getattr(inner.func, "id", None) == "KafkaProducer"):
                    in_loop = True
    check("no KafkaProducer is constructed inside the send loop", not in_loop)


def test_entrypoints_bootstrap_their_own_import_path():
    """streamlit run puts src/ui on sys.path, not the project root."""
    import re as _re
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for rel in (("src", "ui", "dashboard.py"), ("scripts", "smoke_test.py")):
        text = open(os.path.join(root, *rel), encoding="utf-8").read()
        m = _re.search(r"^from src\.", text, _re.MULTILINE)
        prelude = text[:m.start()] if m else text
        check(f"{rel[-1]} adds the project root to sys.path",
              "sys.path.insert" in prelude, "missing bootstrap")


# ------------------------------------------------------ scoring / lift
def test_lift_removes_popularity_bias():
    """
    The failure lift exists to fix: a bestseller co-occurs with everything, so
    raw counts rank it top for every anchor. A niche accessory co-occurs less
    often in absolute terms but far more often than chance.
    """
    total = 10_000.0
    counts = {9001: 1_000.0,    # anchor: laptop
              9002: 5_000.0,    # bestseller phone, seen everywhere
              9003: 200.0}      # niche laptop sleeve
    pairs = [
        {"related_product_id": 9002, "pair_count": 400, "affinity": 400.0},
        {"related_product_id": 9003, "pair_count": 150, "affinity": 150.0},
    ]
    by_affinity = scoring.score_pairs(pairs, counts, total, 9001, "affinity")
    check("by raw affinity the bestseller wins",
          by_affinity[0]["related_product_id"] == 9002)

    by_lift = scoring.score_pairs(pairs, counts, total, 9001, "lift")
    check("by lift the genuinely related accessory wins",
          by_lift[0]["related_product_id"] == 9003,
          str([(r["related_product_id"], r["lift"]) for r in by_lift]))


def test_lift_maths():
    # independent: P(A,B) == P(A)P(B)  ->  lift 1
    check("independent pair has lift 1.0",
          abs(scoring.lift(100, 1000, 1000, 10000) - 1.0) < 1e-9,
          str(scoring.lift(100, 1000, 1000, 10000)))
    check("over-represented pair has lift > 1",
          scoring.lift(300, 1000, 1000, 10000) > 1)
    check("under-represented pair has lift < 1",
          scoring.lift(10, 1000, 1000, 10000) < 1)
    check("pmi is log2 of lift",
          abs(scoring.pmi(300, 1000, 1000, 10000)
              - math.log2(scoring.lift(300, 1000, 1000, 10000))) < 1e-9)


def test_lift_is_undefined_not_zero_for_unseen_products():
    check("unseen product returns None, not 0", scoring.lift(5, 0, 100, 1000) is None)
    check("empty corpus returns None", scoring.lift(5, 10, 10, 0) is None)
    rows = scoring.score_pairs(
        [{"related_product_id": 9, "pair_count": 5, "affinity": 5.0}],
        {}, 0.0, 1, "lift")
    check("undefined score falls back to affinity and is flagged",
          rows[0]["score"] == 5.0 and rows[0]["score_undefined"] is True,
          str(rows[0]))


def test_scoring_rejects_unknown_method():
    try:
        scoring.score_pairs([], {}, 1.0, 1, "vibes")
        check("unknown scoring method raises", False, "no exception")
    except ValueError:
        check("unknown scoring method raises", True)


# ------------------------------------------------- idempotency / replay
def test_replaying_a_batch_does_not_double_count(spark):
    """
    Proves the upsert key is right. Processing the SAME events twice must
    produce identical aggregates - the property that makes a restart after
    failure safe. The original pipeline appended, so a replay doubled the row.
    """
    base = 1_700_000_000.0
    rows = [event(1001, 9001, "view", base + i) for i in range(5)]
    once = T.trending(
        T.valid_events(T.parse_events(as_kafka_rows(spark, rows))), "10 minutes"
    ).collect()

    twice_rows = T.trending(
        T.valid_events(T.parse_events(as_kafka_rows(spark, rows))), "10 minutes"
    ).collect()

    def key(rs):
        return {(r["window_start"], r["product_id"]): r["event_count"]
                for r in rs}
    check("reprocessing identical input yields identical aggregates",
          key(once) == key(twice_rows), f"{key(once)} vs {key(twice_rows)}")

    upsert_key = ["window_start", "window_end", "product_id"]
    check("the upsert key uniquely identifies a trending row",
          len({tuple(r[k] for k in upsert_key) for r in once}) == len(once))


def test_sinks_never_pull_a_batch_into_the_driver():
    """
    batch_df.collect() inside foreachBatch moves the entire micro-batch into
    the driver heap, making driver memory the ceiling on batch size. Every
    sink must write from the executors.
    """
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src = open(os.path.join(root, "src", "streaming", "job.py"),
               encoding="utf-8").read()
    code = "\n".join(line for line in src.splitlines()
                     if not line.strip().startswith("#"))
    tree = ast.parse(src)
    offenders = []
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in {"collect", "toPandas"}):
            offenders.append(f"{node.func.attr} at line {node.lineno}")
    check("no sink calls collect()/toPandas() on a batch",
          not offenders, str(offenders))
    check("sinks write via foreachPartition",
          code.count("foreachPartition") >= 2,
          f"found {code.count('foreachPartition')}")


def test_retention_is_configured():
    """Without TTL indexes the aggregate collections grow forever."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    job = open(os.path.join(root, "src", "streaming", "job.py"),
               encoding="utf-8").read()
    check("TTL index is created on the aggregate collections",
          "expireAfterSeconds" in job)
    check("retention period is configurable",
          "RETENTION_HOURS" in job and hasattr(config, "RETENTION_HOURS"))


def test_no_misleading_unique_user_counts():
    """
    Spark computes approx_count_distinct PER WINDOW. Summing or maxing that
    across windows does not give distinct users over the range, so the field
    must not be presented as one.
    """
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    api = open(os.path.join(root, "src", "api", "main.py"),
               encoding="utf-8").read()
    emitted = [line for line in api.splitlines()
               if '"unique_users":' in line and "$unique_users" not in line]
    check("API never emits a field called unique_users for a multi-window range",
          not emitted, str(emitted))
    check("the honest name is used instead",
          "peak_users_per_window" in api)


def test_dashboard_reads_only_fields_the_api_returns():
    """The API renamed unique_users; the dashboard kept reading the old key
    and the Trending panel raised KeyError as soon as there was data."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ui = open(os.path.join(root, "src", "ui", "dashboard.py"),
              encoding="utf-8").read()
    check("dashboard does not read the removed unique_users field",
          'r["unique_users"]' not in ui)
    check("dashboard reads peak_users_per_window", "peak_users_per_window" in ui)


def test_progress_listener_records_spark_progress():
    """The listener must be constructible and must read the progress OBJECTS
    PySpark hands it. Both were broken, so /metrics never saw Spark."""
    from types import SimpleNamespace as NS
    from src.streaming import job

    try:
        listener = job.make_listener("mongodb://unused", "unused")
        check("progress listener can be instantiated", True)
    except TypeError as exc:
        check("progress listener can be instantiated", False, str(exc))
        return

    written = []
    listener._write = written.append
    event = NS(progress=NS(
        name="trending", batchId=3, inputRowsPerSecond=12.5,
        processedRowsPerSecond=20.0, batchDuration=150,
        stateOperators=[NS(numRowsTotal=7)],
        sources=[NS(metrics={"maxOffsetsBehindLatest": "42"})]))
    try:
        listener.onQueryProgress(event)
    except Exception as exc:                          # noqa: BLE001
        check("listener reads Spark progress objects", False, repr(exc))
        return
    doc = written[0] if written else {}
    check("listener records state-store rows", doc.get("state_rows") == [7],
          str(doc))
    check("listener records Kafka lag as a number",
          doc.get("kafka_lag") == [42.0], str(doc))


def test_kafka_serializers():
    from src.common.kafka_io import JsonValueSerializer, StringKeySerializer
    value, key = JsonValueSerializer(), StringKeySerializer()
    check("value serializer encodes dicts as JSON (kafka-python 3 call)",
          json.loads(value.serialize("t", [], {"a": 1})) == {"a": 1})
    check("value serializer works with the kafka-python 2 call",
          json.loads(value.serialize("t", {"a": 1})) == {"a": 1})
    check("raw bytes pass through, so malformed events stay malformed",
          value.serialize("t", [], b"{bad") == b"{bad")
    check("key serializer encodes ints", key.serialize("t", [], 1001) == b"1001")
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    lambdas = []
    for rel in (("src", "producer", "producer.py"), ("src", "ui", "dashboard.py"),
                ("scripts", "seed.py"), ("scripts", "smoke_test.py"),
                ("scripts", "load_test.py")):
        text = open(os.path.join(root, *rel), encoding="utf-8").read()
        if "serializer=lambda" in text:
            lambdas.append(rel[-1])
    check("no producer uses a deprecated lambda serializer", not lambdas,
          str(lambdas))


def test_pairs_are_not_delayed_by_default():
    """A 5-minute window with a 10-minute gap took ~17 minutes to show the
    first related product; the defaults must keep that to a few minutes."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src = open(os.path.join(root, "src", "common", "config.py"),
               encoding="utf-8").read()
    check("default co-view gap is 2 minutes",
          'os.getenv("CO_VIEW_GAP", "2 minutes")' in src)
    check("default co-occurrence window is 1 minute",
          'os.getenv("COOCCURRENCE_WINDOW", "1 minute")' in src)
    check("a new query reads from the earliest offset",
          'os.getenv("STARTING_OFFSETS", "earliest")' in src)


def test_seeded_v1_events_carry_a_session():
    """Without session_id, seeded v1 events paired by shopper across visits."""
    import importlib.util
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    spec = importlib.util.spec_from_file_location(
        "seed_script", os.path.join(root, "scripts", "seed.py"))
    seed = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(seed)
    events = seed.make_session(1001, 1_700_000_000.0, legacy=True)
    check("seeded v1 events keep a session id",
          all(e.get("session_id") for e in events), str(events[:1]))
    check("seeded v1 events stay v1 (no schema_version)",
          all("schema_version" not in e for e in events))


def test_api_hides_weak_pairs_and_labels_metrics():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    api = open(os.path.join(root, "src", "api", "main.py"),
               encoding="utf-8").read()
    check("related products and graph filter on MIN_PAIR_COUNT",
          api.count("MIN_PAIR_COUNT") >= 2 and "min_pairs" in api)
    check("Spark metrics are labelled per query", '{{query="' in api)
    dash = json.load(open(os.path.join(root, "monitoring", "grafana",
                                       "dashboards", "pipeline.json"),
                          encoding="utf-8"))
    rate = [p for p in dash["panels"] if p["title"] == "Ingest vs processing rate"][0]
    exprs = " ".join(t["expr"] for t in rate["targets"])
    check("the ingest panel charts both read and processed rates",
          "input_rows_per_second" in exprs and "processed_rows_per_second" in exprs,
          exprs)


def test_session_products_follow_the_rules():
    """The demo data's structure comes only from catalog.session_products."""
    import random as _random
    rng = _random.Random(7)
    tidy_ok, repeats = True, False
    for _ in range(500):
        chosen = catalog.session_products(6, 0.0, rng)
        allowed = set(catalog.AFFINITY[chosen[0]["category"]])
        tidy_ok &= all(p["category"] in allowed for p in chosen[1:])
        repeats |= len({p["id"] for p in chosen}) != len(chosen)
    check("with no wandering, sessions stay in related categories", tidy_ok)
    check("a product never repeats within a session", not repeats)

    crossed = 0
    for _ in range(500):
        chosen = catalog.session_products(4, 0.5, rng)
        allowed = set(catalog.AFFINITY[chosen[0]["category"]])
        crossed += any(p["category"] not in allowed for p in chosen[1:])
    check("with wandering, some sessions cross into unrelated categories",
          crossed > 50, f"{crossed} of 500")


def test_compose_runs_the_whole_stack():
    """One `docker compose up` must start every service, in a safe order."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    compose = open(os.path.join(root, "docker-compose.yml"), encoding="utf-8").read()
    for service in ("kafka-init:", "api:", "dashboard:", "producer:", "seed:"):
        check(f"compose defines {service[:-1]}", f"\n  {service}" in compose)
    check("the topic is created with several partitions",
          "--partitions ${KAFKA_PARTITIONS:-6}" in compose)
    check("the producer waits for the backfill to finish",
          "seed:\n        condition: service_completed_successfully" in compose)
    check("checkpoints live in a Docker volume",
          "spark_checkpoints:/checkpoints" in compose)
    anchor = compose.split("x-app:", 1)[1].split("\nservices:", 1)[0]
    check("app services do not share one image name (parallel builds race "
          "to tag it)", "\n  image:" not in anchor, anchor[:200])
    prom = open(os.path.join(root, "monitoring", "prometheus.yml"),
                encoding="utf-8").read()
    alerts = open(os.path.join(root, "monitoring", "alerts.yml"),
                  encoding="utf-8").read()
    api = open(os.path.join(root, "src", "api", "main.py"), encoding="utf-8").read()
    check("Prometheus scrapes the API container by name", '"api:8000"' in prom)
    check("Prometheus loads the alert rules", "alerts.yml" in prom)
    check("every alert metric is emitted by the API",
          all(m in api for m in ("affinity_pipeline_staleness_seconds",
                                 "affinity_kafka_lag_offsets",
                                 "affinity_batch_duration_ms",
                                 "affinity_state_rows")
              if m in alerts))


def test_partition_writers_can_be_shipped_to_workers():
    """
    spark-submit runs job.py as __main__, so cloudpickle copies whatever its
    partition writers refer to BY VALUE. A MongoClient cache defined in job.py
    was copied too; once the driver had opened a client (it holds thread
    locks) every batch failed with "cannot pickle '_thread.lock' object" and
    the job restarted every ~20 s. Loading job.py without registering it as a
    module reproduces the by-value path.
    """
    import importlib.util
    import threading
    from pyspark import cloudpickle
    from src.common import mongo

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    spec = importlib.util.spec_from_file_location(
        "job_as_main", os.path.join(root, "src", "streaming", "job.py"))
    job_main = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(job_main)

    class _HoldsLock:
        def __init__(self):
            self.lock = threading.Lock()

    key = "mongodb://pickle-test"
    # What the driver holds after its first batch. Filled in every cache the
    # job could use, so the check also catches a cache moved back into job.py.
    caches = [mongo._CLIENTS] + [v for k, v in vars(job_main).items()
                                 if k.isupper() and isinstance(v, dict)
                                 and "CLIENT" in k]
    for cache in caches:
        cache[key] = _HoldsLock()
    try:
        for name, writer in (
                ("upsert", job_main.mongo_upsert("c", ["k"])),
                ("append", job_main.mongo_append("c")),
                ("mirror", job_main.mirror_then_upsert("c", ["k"]))):
            try:
                cloudpickle.dumps(writer)
                check(f"{name} writer can be sent to workers while the driver "
                      "holds a MongoDB client", True)
            except Exception as exc:                  # noqa: BLE001
                check(f"{name} writer can be sent to workers while the driver "
                      "holds a MongoDB client", False, str(exc)[:200])
    finally:
        for cache in caches:
            cache.pop(key, None)
    check("job.py keeps no client cache of its own",
          not hasattr(job_main, "_MONGO_CLIENTS"))


def test_state_store_is_available(spark):
    from src.streaming import job
    name = job.state_store_provider()
    check("the configured state store is RocksDB by default",
          config.STATE_STORE != "rocksdb" or name.endswith("RocksDBStateStoreProvider"),
          name)
    try:
        spark.sparkContext._jvm.java.lang.Class.forName(name)
        check("the state store class exists in this Spark", True)
    except Exception as exc:                          # noqa: BLE001
        check("the state store class exists in this Spark", False, str(exc)[:200])


def test_undefined_lift_ranks_below_defined_lift():
    counts = {1: 100.0, 2: 100.0}          # product 3 has no counts
    pairs = [{"related_product_id": 3, "pair_count": 9, "affinity": 50.0},
             {"related_product_id": 2, "pair_count": 1, "affinity": 1.0}]
    ranked = scoring.score_pairs(pairs, counts, 1000.0, 1, "lift")
    check("a pair with a real lift outranks one that fell back to affinity",
          ranked[0]["related_product_id"] == 2,
          str([(r["related_product_id"], r["score"]) for r in ranked]))


def test_env_file_is_loaded():
    check("config loads .env when python-dotenv is available",
          hasattr(config, "ENV_FILE") and config.ENV_FILE.name == ".env")
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    reqs = open(os.path.join(root, "requirements.txt"), encoding="utf-8").read()
    check("python-dotenv is a runtime dependency", "python-dotenv" in reqs)


def test_monitoring_config_is_consistent():
    """A Grafana panel pointing at a datasource uid that does not exist
    renders 'Datasource not found' - silently, and only at demo time."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ds = open(os.path.join(root, "monitoring", "grafana", "provisioning",
                           "datasources", "prometheus.yml"),
              encoding="utf-8").read()
    dash = json.load(open(os.path.join(root, "monitoring", "grafana",
                                       "dashboards", "pipeline.json"),
                          encoding="utf-8"))
    check("datasource declares a fixed uid", "uid: affinity-prometheus" in ds)
    uids = {p["datasource"]["uid"] for p in dash["panels"]}
    check("every panel points at that uid", uids == {"affinity-prometheus"}, str(uids))

    api = open(os.path.join(root, "src", "api", "main.py"),
               encoding="utf-8").read()
    import re as _re
    charted = set()
    for p in dash["panels"]:
        charted |= set(_re.findall(r"affinity_[a-z_]+", p["targets"][0]["expr"]))
    missing = sorted(m for m in charted if m not in api)
    check("every charted metric is actually emitted by the API",
          not missing, f"charted but never emitted: {missing}")


def main():
    test_catalog_consistency()
    test_dashboard_dot_generation()
    test_producer_uses_a_single_kafka_connection()
    test_entrypoints_bootstrap_their_own_import_path()
    test_lift_removes_popularity_bias()
    test_lift_maths()
    test_lift_is_undefined_not_zero_for_unseen_products()
    test_scoring_rejects_unknown_method()
    test_monitoring_config_is_consistent()
    test_sinks_never_pull_a_batch_into_the_driver()
    test_retention_is_configured()
    test_no_misleading_unique_user_counts()
    test_dashboard_reads_only_fields_the_api_returns()
    test_partition_writers_can_be_shipped_to_workers()
    test_undefined_lift_ranks_below_defined_lift()
    test_env_file_is_loaded()
    test_kafka_serializers()
    test_pairs_are_not_delayed_by_default()
    test_seeded_v1_events_carry_a_session()
    test_api_hides_weak_pairs_and_labels_metrics()
    test_session_products_follow_the_rules()
    test_compose_runs_the_whole_stack()
    test_progress_listener_records_spark_progress()
    spark = spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        test_parsing(spark)
        test_garbage_input_does_not_crash(spark)
        test_trending(spark)
        test_trending_separates_windows(spark)
        test_co_occurrence_batch(spark)
        test_v1_and_v2_events_both_parse(spark)
        test_v1_events_still_produce_pairs(spark)
        test_co_occurrence_requires_the_same_session(spark)
        test_events_without_a_session_still_pair(spark)
        test_co_occurrence_ignores_cross_user(spark)
        test_replaying_a_batch_does_not_double_count(spark)
        test_streaming_end_to_end(spark)
        test_join_state_is_evicted_over_time(spark)
        test_state_store_is_available(spark)
    finally:
        spark.stop()

    print()
    print("ALL PASSED" if not FAILURES else f"{len(FAILURES)} FAILED: {FAILURES}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
