"""
Single source of truth for every tunable, driven by environment variables.

Nothing in this project hardcodes a hostname or a port. The same code runs on
the host (``localhost:9092``) and inside Docker (``kafka:29092``) purely by
changing the environment — which is the only reason the producer, the Spark
job and the API can agree on where Kafka is.
"""

import os
from pathlib import Path

# Load <project root>/.env when python-dotenv is installed. Nothing read it
# before, so editing .env silently changed nothing. override=False: values
# already in the environment win, so docker-compose.yml keeps the container's
# own KAFKA_BOOTSTRAP / MONGO_URI even though .env holds the host values.
ENV_FILE = Path(__file__).resolve().parents[2] / ".env"
try:
    from dotenv import load_dotenv as _load_dotenv
except ImportError:                       # optional - the defaults still apply
    _load_dotenv = None                   # type: ignore[assignment]
load_dotenv = _load_dotenv
if load_dotenv is not None and ENV_FILE.is_file():
    load_dotenv(ENV_FILE, override=False)


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc


# --- Kafka ----------------------------------------------------------------
# Host processes use localhost:9092; containers use kafka:29092. Both
# listeners are advertised by the broker (see docker-compose.yml).
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_EVENTS = os.getenv("TOPIC_EVENTS", "clickstream")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "clickstream.dlq")

# --- MongoDB --------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27018")
MONGO_DB = os.getenv("MONGO_DB", "product_affinity")
COLL_TRENDING = "trending"
COLL_PAIRS = "product_pairs"          # co-viewed product pairs per window
COLL_DLQ = "dead_letter"

# --- Streaming windows ----------------------------------------------------
# Every stateful operation is windowed. A streaming aggregation grouped only
# by a business key (user_id, product_id) keeps state for every key it has
# ever seen: the watermark cannot evict it, because event_time is not part of
# the grouping key. Measured on a rate source, that grows without bound
# (0 -> 1600 -> 2400 -> 3200 rows in 12 s) while the windowed equivalent
# stays flat. See README.md, "Bounded state".
WATERMARK = os.getenv("WATERMARK", "2 minutes")
TRENDING_WINDOW = os.getenv("TRENDING_WINDOW", "1 minute")
TRENDING_SLIDE = os.getenv("TRENDING_SLIDE", "")           # "" = tumbling

# Two events in the same session inside this gap are treated as co-viewed.
# Also bounds the stream-stream self-join state.
#
# Together these decide how long related products take to appear. The join
# holds its output back by the gap, so a pair is saved only once newer events
# arrive about  COOCCURRENCE_WINDOW + CO_VIEW_GAP + WATERMARK  later.
# Measured on Spark 3.5 and again on 4.1.3: 5 min / 10 min took ~17 minutes;
# 1 min / 2 min takes ~5. A session lasts seconds (seed.py spreads one over
# at most ~100 s), so a 2-minute gap loses no pairs.
CO_VIEW_GAP = os.getenv("CO_VIEW_GAP", "2 minutes")
COOCCURRENCE_WINDOW = os.getenv("COOCCURRENCE_WINDOW", "1 minute")

TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")

# Where a NEW query starts reading. "earliest", so events sent while the Spark
# job was still starting (the seed runs as soon as Kafka is ready) are not
# skipped.
# Ignored once a checkpoint exists: Spark resumes from its saved offsets.
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/tmp/spark-checkpoints")

# Cap on Kafka records per micro-batch; 0 = no cap. A cap keeps batch times
# predictable when a backlog builds up (after a restart, or under load).
MAX_OFFSETS_PER_TRIGGER = _int("MAX_OFFSETS_PER_TRIGGER", 0)

# Shuffle partitions for the stateful operators. Matches the 8 local cores the
# job is given in docker-compose.yml. Changing it needs fresh checkpoints.
SHUFFLE_PARTITIONS = _int("SHUFFLE_PARTITIONS", 8)

# Where streaming state lives between batches. "rocksdb" keeps it on local
# disk with changelog checkpointing, so large join state does not sit on the
# JVM heap; "hdfs" is Spark's older in-memory provider. Changing it needs
# fresh checkpoints.
STATE_STORE = os.getenv("STATE_STORE", "rocksdb").strip().lower()
STATE_STORE_PROVIDERS = {
    "rocksdb": "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
    "hdfs": "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
}

# --- Producer -------------------------------------------------------------
EVENTS_PER_SECOND = _float("EVENTS_PER_SECOND", 20.0)
SESSION_MIN_EVENTS = _int("SESSION_MIN_EVENTS", 2)
SESSION_MAX_EVENTS = _int("SESSION_MAX_EVENTS", 6)
MALFORMED_RATE = _float("MALFORMED_RATE", 0.01)   # exercise the DLQ path

# Chance that each further product in a session comes from ANY category
# rather than a related one. Real shoppers wander (a phone, then shoes), so the
# demo data should too; lift is what keeps those chance pairs from ranking
# above genuine ones. 0 reproduces perfectly tidy sessions.
CROSS_CATEGORY_RATE = _float("CROSS_CATEGORY_RATE", 0.15)

# Fraction of sessions emitted in the v2 wire format. The default puts BOTH
# versions on the topic at once, which is what a partially-rolled-out producer
# fleet looks like. Set 0.0 for all-v1, 1.0 for all-v2.
SCHEMA_V2_RATIO = _float("SCHEMA_V2_RATIO", 0.5)
CHANNELS = ("web", "ios", "android")

# --- API ------------------------------------------------------------------
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = _int("API_PORT", 8000)
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
DEFAULT_LIMIT = _int("DEFAULT_LIMIT", 10)

# How far back /related-products aggregates. Summing every window ever written
# makes the numbers grow without bound and turns the ranking into an all-time
# popularity chart; recent affinity is what a shopper cares about.
PAIR_LOOKBACK_MINUTES = _int("PAIR_LOOKBACK_MINUTES", 30)

# Pairs seen fewer times than this over the lookback are treated as noise and
# left out of /related-products and /graph. Two chance co-views (lift ~0.02)
# are not a real relationship.
MIN_PAIR_COUNT = _int("MIN_PAIR_COUNT", 3)

# How much history /trending sums over, in minutes.
#
# Spark writes cheap 1-minute tumbling windows; the API adds up the last N of
# them. Returning only the newest window makes the display collapse whenever
# traffic pauses - stop the producer, click two products, and "trending" shows
# exactly those two. Nothing is lost in that case (every past window is still
# in Mongo), the query was simply too narrow.
#
# The alternative - a 1-hour window sliding every minute - puts each event in
# 60 windows, so 60x the state and 60x the output rows, for the same answer.
TRENDING_LOOKBACK_MINUTES = _int("TRENDING_LOOKBACK_MINUTES", 30)

# Schema version the producer stamps on new events. Older events without the
# field are treated as v1 and still parse - see docs/adr/0006.
SCHEMA_VERSION = _int("SCHEMA_VERSION", 2)
# Fraction of events deliberately emitted in the OLD v1 format, to prove the
# pipeline tolerates a partially-upgraded fleet of producers.
LEGACY_EVENT_RATE = _float("LEGACY_EVENT_RATE", 0.15)

# How long aggregate collections are kept. Enforced by MongoDB TTL indexes:
# without them the pipeline writes a row per window per product forever.
RETENTION_HOURS = _int("RETENTION_HOURS", 48)

# Read-through cache TTL for /trending and /related-products. The underlying
# data only changes once per trigger interval, so serving every request with
# a fresh Mongo aggregation is wasted work.
CACHE_TTL_SECONDS = _float("CACHE_TTL_SECONDS", 5.0)

# The graph is the one expensive read: it groups every pair row inside the
# lookback, and after a replayed month that is over a million of them - 5-6
# seconds, measured. Recomputing that every five seconds would keep one core
# busy for nobody's benefit, so it gets its own, longer TTL.
GRAPH_CACHE_SECONDS = _float("GRAPH_CACHE_SECONDS", 30.0)

EVENT_TYPES = ("view", "click", "add_to_cart", "purchase", "search")

# Relative weight of each event type when scoring co-occurrence. A purchase
# is far stronger evidence of product affinity than a search.
EVENT_WEIGHTS = {
    "view": 1.0,
    "search": 0.5,
    "click": 1.5,
    "add_to_cart": 3.0,
    "purchase": 5.0,
}


def validate() -> None:
    if EVENTS_PER_SECOND <= 0:
        raise ValueError("EVENTS_PER_SECOND must be positive.")
    if SESSION_MIN_EVENTS < 1 or SESSION_MAX_EVENTS < SESSION_MIN_EVENTS:
        raise ValueError("Require 1 <= SESSION_MIN_EVENTS <= SESSION_MAX_EVENTS.")
    if not 0.0 <= MALFORMED_RATE < 1.0:
        raise ValueError("MALFORMED_RATE must be in [0, 1).")
    if not 0.0 <= SCHEMA_V2_RATIO <= 1.0:
        raise ValueError("SCHEMA_V2_RATIO must be in [0, 1].")
    if not 0.0 <= CROSS_CATEGORY_RATE <= 1.0:
        raise ValueError("CROSS_CATEGORY_RATE must be in [0, 1].")
    if STATE_STORE not in STATE_STORE_PROVIDERS:
        raise ValueError(f"STATE_STORE must be one of {sorted(STATE_STORE_PROVIDERS)}.")
    if SHUFFLE_PARTITIONS < 1 or MAX_OFFSETS_PER_TRIGGER < 0:
        raise ValueError("SHUFFLE_PARTITIONS must be >= 1 and MAX_OFFSETS_PER_TRIGGER >= 0.")
    missing = set(EVENT_TYPES) - set(EVENT_WEIGHTS)
    if missing:
        raise ValueError(f"EVENT_WEIGHTS is missing weights for {sorted(missing)}.")


validate()
