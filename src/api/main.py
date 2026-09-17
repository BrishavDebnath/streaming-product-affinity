"""
Serving layer.

    uvicorn src.api.main:app --reload --port 8000

Endpoints
    GET /health                     liveness + Mongo reachability
    GET /trending                   top products in the most recent window
    GET /related-products/{id}     products viewed in the same sessions, with trending fallback
    GET /stats                      pipeline counters, including DLQ size
    GET /metrics                    Prometheus text format
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from fastapi import FastAPI, HTTPException, Query
from pymongo import DESCENDING, MongoClient
from pymongo.errors import PyMongoError

from src.common import catalog, config, scoring

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s api | %(message)s")
log = logging.getLogger("api")

app = FastAPI(
    title="Streaming Product Affinity API",
    description="Serves trending products and co-viewed (related) products "
                "computed by a Spark Structured Streaming pipeline.",
    version="1.0.0",
)

_client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=3000)
_db = _client[config.MONGO_DB]

_CACHE: Dict[str, tuple] = {}


def _cached(key: str, builder):
    """
    Tiny read-through cache. The underlying aggregates only change once per
    trigger interval, so recomputing a Mongo aggregation on every request is
    wasted work - and under load the API, not Spark, becomes the bottleneck.
    """
    now = time.time()
    hit = _CACHE.get(key)
    if hit and now - hit[0] < config.CACHE_TTL_SECONDS:
        _COUNTERS["cache_hits"] += 1
        return hit[1]
    value = builder()
    _CACHE[key] = (now, value)
    _COUNTERS["cache_misses"] += 1
    return value


_COUNTERS: Dict[str, int] = {
    "cache_hits": 0,
    "cache_misses": 0,
    "requests_total": 0,
    "related_served": 0,
    "related_fallback": 0,
    "errors_total": 0,
}


def _latest_window() -> Dict:
    """The most recent completed trending window, or {} if none yet."""
    doc = _db[config.COLL_TRENDING].find_one(sort=[("window_start", DESCENDING)])
    return doc or {}


@app.get("/health")
def health():
    _COUNTERS["requests_total"] += 1
    try:
        _client.admin.command("ping")
        mongo_ok = True
        detail = None
    except PyMongoError as exc:
        mongo_ok = False
        detail = str(exc)
        _COUNTERS["errors_total"] += 1
    body = {"status": "ok" if mongo_ok else "degraded",
            "mongo_reachable": mongo_ok, "database": config.MONGO_DB}
    if detail:
        body["detail"] = detail
    return body


@app.get("/trending")
def trending(limit: int = Query(default=config.DEFAULT_LIMIT, ge=1, le=100),
             minutes: int = Query(default=config.TRENDING_LOOKBACK_MINUTES,
                                  ge=1, le=1440)):
    """
    Top products in the most recent window, sorted by weighted score.

    The original version returned `list(collection.find({}))` unsorted and
    unlimited, mixing every window ever written together. The TODO comment
    asking for a descending sort was never implemented.
    """
    _COUNTERS["requests_total"] += 1
    latest = _latest_window()
    if not latest:
        return {"window_start": None, "window_end": None, "minutes": minutes,
                "count": 0, "trending": [],
                "message": "No completed windows yet. Is the producer running?"}

    # Sum across the last `minutes` of 1-minute windows rather than reading
    # only the newest one, so a lull in traffic does not empty the display.
    return _cached(f"trending:{limit}:{minutes}",
                   lambda: _trending_uncached(limit, minutes, latest))


def _trending_uncached(limit, minutes, latest):
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    pipeline = [
        {"$match": {"window_start": {"$gte": cutoff}}},
        {"$group": {"_id": "$product_id",
                    "event_count": {"$sum": "$event_count"},
                    "score": {"$sum": "$score"},
                    # NOT distinct users over the whole range. Spark computes
                    # approx_count_distinct PER WINDOW; the max of thirty
                    # approximate per-minute counts is not the distinct count
                    # over thirty minutes. Reporting it as "unique users"
                    # would be a wrong number on the dashboard. Naming it for
                    # what it actually is keeps it useful and honest.
                    # An exact figure needs HLL sketches merged at read time.
                    "peak_users_per_window": {"$max": "$unique_users"},
                    "windows": {"$sum": 1},
                    "first_window": {"$min": "$window_start"},
                    "last_window": {"$max": "$window_end"}}},
        {"$sort": {"score": DESCENDING}},
        {"$limit": limit},
    ]
    try:
        agg = list(_db[config.COLL_TRENDING].aggregate(pipeline))
    except PyMongoError as exc:
        _COUNTERS["errors_total"] += 1
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    rows = [{"product_id": d["_id"],
             "event_count": d["event_count"],
             "score": round(d["score"], 2),
             "peak_users_per_window": d.get("peak_users_per_window"),
             "windows": d["windows"]} for d in agg]

    return {"window_start": agg[0]["first_window"] if agg else None,
            "window_end": latest.get("window_end"),
            "minutes": minutes,
            "count": len(rows),
            "trending": catalog.enrich(rows)}


@app.get("/related-products/{product_id}")
def related_products(product_id: int,
                    limit: int = Query(default=config.DEFAULT_LIMIT, ge=1, le=50),
                    score_by: str = Query(default="affinity",
                                          pattern="^(affinity|lift|pmi)$")):
    """
    "Users who interacted with this product also interacted with..."

    Falls back to trending when a product has no co-occurrence data yet — the
    cold-start case every related-items feature has to answer for. The
    response says which path was taken rather than pretending they are the
    same thing.
    """
    _COUNTERS["requests_total"] += 1
    if catalog.get(product_id) is None:
        raise HTTPException(status_code=404,
                            detail=f"Unknown product_id {product_id}")

    cutoff = datetime.now(timezone.utc) - timedelta(
        minutes=config.PAIR_LOOKBACK_MINUTES)
    pipeline = [
        {"$match": {"product_id": product_id, "window_start": {"$gte": cutoff}}},
        {"$group": {"_id": "$related_product_id",
                    "affinity": {"$sum": "$affinity"},
                    "pair_count": {"$sum": "$pair_count"},
                    "peak_users_per_window": {"$max": "$unique_users"}}},
        {"$match": {"pair_count": {"$gte": config.MIN_PAIR_COUNT}}},
        {"$sort": {"affinity": -1}},
        # Over-fetch: re-ranking by lift can promote a low-count pair, so the
        # top-`limit` by affinity is not the top-`limit` by lift.
        {"$limit": max(limit * 4, 20)},
    ]
    try:
        agg = list(_db[config.COLL_PAIRS].aggregate(pipeline))
        if not agg:      # nothing recent - fall back to the full history
            pipeline[0] = {"$match": {"product_id": product_id}}
            agg = list(_db[config.COLL_PAIRS].aggregate(pipeline))
    except PyMongoError as exc:
        _COUNTERS["errors_total"] += 1
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if agg:
        _COUNTERS["related_served"] += 1
        candidates = [{"related_product_id": d["_id"],
                       "affinity": round(d["affinity"], 3),
                       "pair_count": d["pair_count"],
                       "peak_users_per_window":
                           d.get("peak_users_per_window")} for d in agg]

        counts, total = _product_counts(cutoff)
        ranked = scoring.score_pairs(candidates, counts, total,
                                     anchor_id=product_id, method=score_by)[:limit]
        rows = [{"product_id": r["related_product_id"],
                 "affinity": r["affinity"], "pair_count": r["pair_count"],
                 "peak_users_per_window": r.get("peak_users_per_window"),
                 "lift": r.get("lift"), "pmi": r.get("pmi"),
                 "score": round(r["score"], 4)} for r in ranked]
        return {"product_id": product_id,
                "source": "co_occurrence",
                "ranked_by": score_by,
                "lookback_minutes": config.PAIR_LOOKBACK_MINUTES,
                "count": len(rows),
                "related_products": catalog.enrich(rows)}

    # Cold start: no pairs seen for this product yet.
    _COUNTERS["related_fallback"] += 1
    latest = _latest_window()
    fallback: List[Dict] = []
    if latest:
        fallback = list(_db[config.COLL_TRENDING].find(
            {"window_start": latest["window_start"],
             "product_id": {"$ne": product_id}},
            {"_id": 0},
        ).sort("score", DESCENDING).limit(limit))

    return {"product_id": product_id,
            "source": "trending_fallback",
            "count": len(fallback),
            "related_products": catalog.enrich(fallback),
            "message": "No co-occurrence data for this product yet; "
                       "showing trending products instead."}


def _product_counts(cutoff):
    """
    Per-product interaction counts and the grand total over the lookback
    window, read from the trending collection. These are the marginals that
    lift needs to divide popularity out of a raw co-occurrence count.
    """
    pipeline = [
        {"$match": {"window_start": {"$gte": cutoff}}},
        {"$group": {"_id": "$product_id", "events": {"$sum": "$event_count"}}},
    ]
    try:
        rows = list(_db[config.COLL_TRENDING].aggregate(pipeline))
    except PyMongoError:
        return {}, 0.0
    counts = {r["_id"]: float(r["events"]) for r in rows}
    return counts, float(sum(counts.values()))


@app.get("/throughput")
def throughput(windows: int = Query(default=20, ge=1, le=200)):
    """
    Events per time window, newest first — the pipeline's own measured
    throughput, read back from what it actually wrote rather than from a
    counter in the producer.
    """
    _COUNTERS["requests_total"] += 1
    pipeline = [
        {"$group": {"_id": {"start": "$window_start", "end": "$window_end"},
                    "events": {"$sum": "$event_count"},
                    "score": {"$sum": "$score"},
                    "products": {"$sum": 1},
                    "written_at": {"$max": "$_updated_at"}}},
        {"$sort": {"_id.start": -1}},
        {"$limit": windows},
    ]
    try:
        rows = list(_db[config.COLL_TRENDING].aggregate(pipeline))
    except PyMongoError as exc:
        _COUNTERS["errors_total"] += 1
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    series = []
    for row in rows:
        start, end = row["_id"]["start"], row["_id"]["end"]
        seconds = (end - start).total_seconds() if start and end else None
        series.append({
            "window_start": start,
            "window_end": end,
            "events": row["events"],
            "products": row["products"],
            "score": round(row["score"], 2) if row["score"] is not None else None,
            "events_per_second": round(row["events"] / seconds, 2)
            if seconds else None,
            "written_at": row.get("written_at"),
        })
    return {"count": len(series), "series": list(reversed(series))}


def _pipeline_status():
    """
    End-to-end processing delay and data freshness, shared by /pipeline and
    /metrics.

    `lag_seconds` is (time the row was last written) - (time the window
    closed), measured on the newest window that has closed.
    """
    coll = _db[config.COLL_TRENDING]
    latest = coll.find_one(sort=[("_updated_at", DESCENDING)])
    if not latest or not latest.get("_updated_at"):
        return {"status": "warming_up", "lag_seconds": None,
                "staleness_seconds": None,
                "message": "Waiting for the first results from Spark."}

    def _utc(value):
        if value is not None and value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    now = datetime.now(timezone.utc)
    written = _utc(latest["_updated_at"])
    staleness = (now - written).total_seconds()

    # Trending runs in update mode, so a window is rewritten while it is still
    # open. Measuring on that row gave a NEGATIVE lag. Lag is measured on the
    # newest window whose last write came after it ended.
    closed = coll.find_one(
        {"$expr": {"$gte": ["$_updated_at", "$window_end"]}},
        sort=[("window_end", DESCENDING)])
    lag = window_end = None
    if closed:
        window_end = _utc(closed.get("window_end"))
        lag = (_utc(closed["_updated_at"]) - window_end).total_seconds()

    return {
        "status": "ok" if staleness < 120 else "stale",
        "window_end": window_end,
        "written_at": written,
        "lag_seconds": round(lag, 2) if lag is not None else None,
        "staleness_seconds": round(staleness, 2),
        "trigger_interval": config.TRIGGER_INTERVAL,
        "watermark": config.WATERMARK,
    }


@app.get("/pipeline")
def pipeline_health():
    """End-to-end processing delay - see _pipeline_status."""
    _COUNTERS["requests_total"] += 1
    try:
        return _pipeline_status()
    except PyMongoError as exc:
        _COUNTERS["errors_total"] += 1
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/graph")
def graph(limit: int = Query(default=25, ge=1, le=200),
          min_affinity: float = Query(default=0.0, ge=0.0),
          min_pairs: int = Query(default=config.MIN_PAIR_COUNT, ge=1)):
    """
    Co-occurrence as a node-link graph.

    Only canonical edges (product_id < related_product_id) are returned, so an
    undirected renderer draws each relationship once instead of twice.
    """
    _COUNTERS["requests_total"] += 1
    pipeline = [
        {"$match": {"$expr": {"$lt": ["$product_id", "$related_product_id"]}}},
        {"$group": {"_id": {"a": "$product_id", "b": "$related_product_id"},
                    "affinity": {"$sum": "$affinity"},
                    "pair_count": {"$sum": "$pair_count"}}},
        {"$match": {"affinity": {"$gte": min_affinity},
                    "pair_count": {"$gte": min_pairs}}},
        {"$sort": {"affinity": -1}},
        {"$limit": limit},
    ]
    try:
        rows = list(_db[config.COLL_PAIRS].aggregate(pipeline))
    except PyMongoError as exc:
        _COUNTERS["errors_total"] += 1
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    edges, node_ids = [], set()
    for row in rows:
        a, b = row["_id"]["a"], row["_id"]["b"]
        node_ids.update((a, b))
        edges.append({"source": a, "target": b,
                      "affinity": round(row["affinity"], 3),
                      "pair_count": row["pair_count"]})

    nodes = []
    for pid in sorted(node_ids):
        product = catalog.get(pid)
        degree = sum(1 for e in edges if pid in (e["source"], e["target"]))
        nodes.append({"id": pid,
                      "name": product["name"] if product else str(pid),
                      "category": product["category"] if product else "unknown",
                      "degree": degree})
    return {"nodes": nodes, "edges": edges,
            "node_count": len(nodes), "edge_count": len(edges)}


@app.get("/stats")
def stats():
    _COUNTERS["requests_total"] += 1
    try:
        return {
            "database": config.MONGO_DB,
            "collections": {
                config.COLL_TRENDING: _db[config.COLL_TRENDING].estimated_document_count(),
                config.COLL_PAIRS: _db[config.COLL_PAIRS].estimated_document_count(),
                config.COLL_DLQ: _db[config.COLL_DLQ].estimated_document_count(),
            },
            "latest_window": _latest_window().get("window_start"),
            "counters": dict(_COUNTERS),
            "catalog_size": len(catalog.PRODUCTS),
        }
    except PyMongoError as exc:
        _COUNTERS["errors_total"] += 1
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/metrics")
def metrics():
    """Prometheus text exposition format."""
    lines = [
        "# HELP affinity_requests_total Total API requests served.",
        "# TYPE affinity_requests_total counter",
        f"affinity_requests_total {_COUNTERS['requests_total']}",
        "# HELP affinity_related_products_total Related-product responses by source.",
        "# TYPE affinity_related_products_total counter",
        f'affinity_related_products_total{{source="co_occurrence"}} '
        f'{_COUNTERS["related_served"]}',
        f'affinity_related_products_total{{source="trending_fallback"}} '
        f'{_COUNTERS["related_fallback"]}',
        "# HELP affinity_cache_total API read-through cache outcomes.",
        "# TYPE affinity_cache_total counter",
        f'affinity_cache_total{{result="hit"}} {_COUNTERS["cache_hits"]}',
        f'affinity_cache_total{{result="miss"}} {_COUNTERS["cache_misses"]}',
        "# HELP affinity_errors_total Errors encountered.",
        "# TYPE affinity_errors_total counter",
        f"affinity_errors_total {_COUNTERS['errors_total']}",
    ]
    # One reading per Spark query. Reporting only the newest document mixed the
    # three queries: the input rate jumped between ~19/s (trending) and ~39/s
    # (product_pairs, whose self-join reads the stream twice).
    since = datetime.now(timezone.utc) - timedelta(minutes=5)
    try:
        per_query = list(_db["pipeline_metrics"].aggregate([
            {"$match": {"recorded_at": {"$gte": since}}},
            {"$sort": {"recorded_at": DESCENDING}},
            {"$group": {"_id": "$query", "doc": {"$first": "$$ROOT"}}},
            {"$sort": {"_id": 1}},
        ]))
    except PyMongoError:
        per_query = []

    def _series(metric, help_text, pick):
        rows = []
        for row in per_query:
            value = pick(row["doc"])
            if value is not None:
                rows.append(f'{metric}{{query="{row["_id"]}"}} {value}')
        if rows:
            lines.extend([f"# HELP {metric} {help_text}",
                          f"# TYPE {metric} gauge", *rows])

    def _known(values):
        return [v for v in (values or []) if v is not None]

    _series("affinity_input_rows_per_second",
            "Rows read from Kafka per second, per Spark query.",
            lambda d: d.get("input_rows_per_second"))
    _series("affinity_processed_rows_per_second",
            "Rows processed per second, per Spark query.",
            lambda d: d.get("processed_rows_per_second"))
    _series("affinity_batch_duration_ms",
            "Duration of the latest micro-batch, per Spark query.",
            lambda d: d.get("batch_duration_ms"))
    _series("affinity_kafka_lag_offsets",
            "Events not yet read, measured after each batch against the "
            "broker's latest offsets, per Spark query.",
            lambda d: max(_known(d.get("kafka_lag")), default=None))
    _series("affinity_state_rows",
            "Rows held in streaming state, per Spark query.",
            lambda d: sum(_known(d.get("state_rows"))) if _known(d.get("state_rows")) else None)

    # Freshness gauges drive the PipelineStale alert (monitoring/alerts.yml).
    try:
        status = _pipeline_status()
        for key, metric, help_text in (
                ("staleness_seconds", "affinity_pipeline_staleness_seconds",
                 "Seconds since Spark last saved any result."),
                ("lag_seconds", "affinity_processing_delay_seconds",
                 "Seconds between a window closing and its final write.")):
            if status.get(key) is not None:
                lines += [f"# HELP {metric} {help_text}",
                          f"# TYPE {metric} gauge", f"{metric} {status[key]}"]
    except PyMongoError:
        pass

    try:
        dlq = _db[config.COLL_DLQ].estimated_document_count()
        lines += ["# HELP affinity_dead_letter_events Malformed events captured.",
                  "# TYPE affinity_dead_letter_events gauge",
                  f"affinity_dead_letter_events {dlq}"]
    except PyMongoError:
        pass
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse("\n".join(lines) + "\n")
