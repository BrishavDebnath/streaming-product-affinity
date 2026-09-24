"""
API tests against a fake MongoDB (mongomock) - no stack needed.

`tests/test_transforms.py` checks the Spark side and needs a real
SparkSession; this file checks what the serving layer returns, which is what
the dashboard and any client actually see:

    pytest tests/test_api.py

The Spark container never runs these (it has no FastAPI); CI does.
"""

import importlib
from datetime import datetime, timedelta, timezone

import mongomock
import pytest
from fastapi.testclient import TestClient

from src.common import config

LAPTOP, XPS, SLEEVE, HUB = 9001, 9002, 9003, 9004
PHONE = 9005
UNKNOWN = 424_242


@pytest.fixture()
def api():
    """A fresh API with an empty fake database, per test."""
    import src.api.main as main

    importlib.reload(main)                  # clear caches and counters
    main._client = mongomock.MongoClient()
    main._db = main._client[config.MONGO_DB]
    return main


@pytest.fixture()
def client(api):
    return TestClient(api.app)


def minute(offset):
    """Window start `offset` minutes ago, on the minute."""
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    return now - timedelta(minutes=offset)


def add_trending(api, product, offset=1, events=10, score=None, users=3):
    start = minute(offset)
    api._db[config.COLL_TRENDING].insert_one({
        "window_start": start, "window_end": start + timedelta(minutes=1),
        "product_id": product, "event_count": events,
        "score": float(score if score is not None else events),
        "unique_users": users, "_updated_at": datetime.now(timezone.utc)})


def add_pair(api, a, b, offset=1, count=10, affinity=None, users=2):
    start = minute(offset)
    for first, second in ((a, b), (b, a)):          # the sink mirrors pairs
        api._db[config.COLL_PAIRS].insert_one({
            "window_start": start, "window_end": start + timedelta(minutes=1),
            "product_id": first, "related_product_id": second,
            "pair_count": count,
            "affinity": float(affinity if affinity is not None else count),
            "unique_users": users, "_updated_at": datetime.now(timezone.utc)})


# ------------------------------------------------------------------- basics
def test_health_reports_mongo(client):
    body = client.get("/health").json()
    assert body["status"] == "ok" and body["mongo_reachable"] is True
    assert body["database"] == config.MONGO_DB


def test_empty_database_explains_itself_instead_of_erroring(client):
    trending = client.get("/trending")
    assert trending.status_code == 200
    assert trending.json()["count"] == 0
    assert "producer" in trending.json()["message"].lower()

    pipeline = client.get("/pipeline")
    assert pipeline.status_code == 200
    assert pipeline.json()["status"] == "warming_up"

    graph = client.get("/graph")
    assert graph.status_code == 200 and graph.json()["edge_count"] == 0


def test_an_empty_lookback_is_not_the_same_as_no_data(api, client):
    """After a replay stops, /trending is legitimately empty - but the
    database is not. Saying "no data yet" there sends someone looking for a
    broken pipeline."""
    add_trending(api, LAPTOP, offset=90)            # an hour and a half ago

    body = client.get("/trending?minutes=30").json()
    assert body["count"] == 0
    assert "No events in the last 30 minutes" in body["message"]
    assert body["newest_window_age_minutes"] == pytest.approx(89, abs=2)

    wider = client.get("/trending?minutes=240").json()
    assert wider["count"] == 1 and "message" not in wider


def test_removed_recommendations_path_is_gone(client):
    """The endpoint was renamed in Phase 1; nothing should still answer it."""
    assert client.get(f"/recommendations/{LAPTOP}").status_code == 404


# ----------------------------------------------------------------- trending
def test_trending_sums_windows_and_sorts_by_score(api, client):
    for offset in (1, 2, 3):
        add_trending(api, LAPTOP, offset, events=10, score=10)
    add_trending(api, PHONE, 1, events=25, score=25)

    body = client.get("/trending?limit=5").json()
    rows = body["trending"]
    assert [r["product_id"] for r in rows] == [LAPTOP, PHONE]
    assert rows[0]["event_count"] == 30 and rows[0]["score"] == 30
    assert rows[0]["windows"] == 3
    assert rows[0]["name"] == "Apple MacBook Air M3"      # catalogue joined in


def test_trending_ignores_windows_outside_the_lookback(api, client):
    add_trending(api, LAPTOP, offset=1)
    add_trending(api, PHONE, offset=120)                  # two hours ago
    rows = client.get("/trending?minutes=30").json()["trending"]
    assert [r["product_id"] for r in rows] == [LAPTOP]


def test_trending_never_reports_unique_users_across_windows(api, client):
    for offset in (1, 2):
        add_trending(api, LAPTOP, offset, users=40)
    row = client.get("/trending").json()["trending"][0]
    assert "unique_users" not in row
    assert row["peak_users_per_window"] == 40


def test_repeated_reads_are_served_from_cache(api, client):
    add_trending(api, LAPTOP)
    client.get("/trending")
    client.get("/trending")
    counters = client.get("/stats").json()["counters"]
    assert counters["cache_misses"] == 1 and counters["cache_hits"] == 1


# --------------------------------------------------------- related products
def test_related_products_come_from_co_occurrence(api, client):
    add_pair(api, LAPTOP, SLEEVE, count=20, affinity=30)
    add_pair(api, LAPTOP, HUB, count=10, affinity=12)
    add_trending(api, LAPTOP)

    body = client.get(f"/related-products/{LAPTOP}").json()
    assert body["source"] == "co_occurrence"
    assert [r["product_id"] for r in body["related_products"]] == [SLEEVE, HUB]
    assert body["related_products"][0]["name"] == 'Laptop Sleeve 13"'


def test_a_product_is_never_related_to_itself(api, client):
    add_pair(api, LAPTOP, SLEEVE, count=20)
    body = client.get(f"/related-products/{LAPTOP}").json()
    assert LAPTOP not in [r["product_id"] for r in body["related_products"]]


def test_weak_pairs_are_hidden(api, client):
    """Below MIN_PAIR_COUNT a pair is noise, not a recommendation."""
    add_pair(api, LAPTOP, SLEEVE, count=config.MIN_PAIR_COUNT - 1)
    add_trending(api, PHONE)
    body = client.get(f"/related-products/{LAPTOP}").json()
    assert body["source"] == "trending_fallback"


def test_cold_start_falls_back_to_trending_and_says_so(api, client):
    add_trending(api, PHONE, events=50)
    body = client.get(f"/related-products/{LAPTOP}").json()
    assert body["source"] == "trending_fallback"
    assert [r["product_id"] for r in body["related_products"]] == [PHONE]


def test_empty_slots_are_filled_from_the_query_category(api, client):
    """Co-occurrence rarely has a full list to give. The empty slots go to
    the query's own category first: an offline check on RetailRocket found
    that fill worth far more than trending."""
    add_pair(api, LAPTOP, SLEEVE, count=20)
    add_trending(api, XPS, events=30)          # a laptop, like the query
    add_trending(api, PHONE, events=50)        # busier, but another category
    body = client.get(f"/related-products/{LAPTOP}?limit=3").json()
    assert body["source"] == "co_occurrence"
    rows = body["related_products"]
    assert [r["product_id"] for r in rows] == [SLEEVE, XPS]
    assert [r["source"] for r in rows] == ["co_occurrence", "category_bestsellers"]
    assert body["filled_from_category"] == 1
    assert body["count"] == 2


def test_the_category_fill_never_repeats_a_pair_or_the_query(api, client):
    add_pair(api, LAPTOP, XPS, count=20)
    add_trending(api, XPS, events=30)
    add_trending(api, LAPTOP, events=90)
    body = client.get(f"/related-products/{LAPTOP}?limit=5").json()
    ids = [r["product_id"] for r in body["related_products"]]
    assert ids == [XPS], "XPS once, from pairs; the query never"
    assert body["filled_from_category"] == 0


def test_cold_start_prefers_the_query_category_then_trending(api, client):
    add_trending(api, XPS, events=5)
    add_trending(api, PHONE, events=50)
    body = client.get(f"/related-products/{LAPTOP}?limit=2").json()
    assert body["source"] == "category_fallback"
    rows = body["related_products"]
    assert [r["product_id"] for r in rows] == [XPS, PHONE]
    assert [r["source"] for r in rows] == ["category_bestsellers", "trending"]
    assert "category" in body["message"]


def test_the_cache_does_not_grow_without_end(api, client):
    """Keys carry query parameters and the lookback minute, so the key space
    grows on its own. Nothing used to remove an entry, on a catalogue of tens
    of thousands of items."""
    import src.api.main as main

    add_trending(api, LAPTOP)
    main._CACHE.clear()
    for n in range(main.CACHE_MAX_KEYS + 40):
        client.get(f"/trending?limit={(n % 50) + 1}&minutes={(n % 90) + 1}")
    assert len(main._CACHE) <= main.CACHE_MAX_KEYS
    assert main._COUNTERS["cache_entries"] == len(main._CACHE)

    # Expired entries go even when the cache is far from full.
    main._CACHE.clear()
    main._CACHE["stale"] = (0.0, "old")           # epoch 1970
    client.get("/trending")
    assert "stale" not in main._CACHE


def test_trending_reports_the_range_it_actually_summed(api, client):
    """It used to take the first window of whichever product ranked first,
    and an end from the window still being written, which is in the future."""
    for offset in range(1, 6):
        add_trending(api, XPS, offset=offset, events=5)
    add_trending(api, LAPTOP, offset=1, events=500)   # ranks first, one window
    body = client.get("/trending?limit=5&minutes=30").json()
    started = datetime.fromisoformat(body["window_start"])
    ended = datetime.fromisoformat(body["window_end"])
    if started.tzinfo is None:                 # mongomock hands back naive UTC
        started, ended = started.replace(tzinfo=timezone.utc), ended.replace(tzinfo=timezone.utc)
    assert started == min(minute(o) for o in range(1, 6)), "the oldest window summed"
    assert ended <= datetime.now(timezone.utc), "never a window from the future"


def test_unknown_product_is_rejected(client):
    response = client.get(f"/related-products/{UNKNOWN}")
    assert response.status_code == 404
    assert str(UNKNOWN) in response.json()["detail"]


def test_ranking_method_is_chosen_by_the_caller(api, client):
    # The bestseller co-occurs more often, the accessory more than chance.
    add_pair(api, LAPTOP, PHONE, count=40, affinity=40)
    add_pair(api, LAPTOP, SLEEVE, count=20, affinity=20)
    for product, events in ((PHONE, 10_000), (SLEEVE, 100), (LAPTOP, 500)):
        add_trending(api, product, events=events, score=events)

    by_affinity = client.get(f"/related-products/{LAPTOP}").json()
    by_lift = client.get(f"/related-products/{LAPTOP}?score_by=lift").json()
    assert by_affinity["ranked_by"] == "affinity"
    assert by_affinity["related_products"][0]["product_id"] == PHONE
    assert by_lift["ranked_by"] == "lift"
    assert by_lift["related_products"][0]["product_id"] == SLEEVE


def test_unknown_ranking_method_is_a_client_error(client):
    response = client.get(f"/related-products/{LAPTOP}?score_by=magic")
    assert response.status_code in (400, 422)


# -------------------------------------------------------------------- graph
def test_graph_returns_each_pair_once_with_its_products(api, client):
    add_pair(api, LAPTOP, SLEEVE, count=20)
    add_pair(api, LAPTOP, XPS, count=5)

    body = client.get("/graph?limit=10").json()
    edges = {(e["source"], e["target"]) for e in body["edges"]}
    assert edges == {(LAPTOP, SLEEVE), (LAPTOP, XPS)}     # no mirrored copies
    assert all(e["source"] < e["target"] for e in body["edges"])
    assert body["node_count"] == 3
    assert {n["category"] for n in body["nodes"]} == {"laptop", "laptop-acc"}


def test_graph_hides_weak_pairs_too(api, client):
    add_pair(api, LAPTOP, SLEEVE, count=config.MIN_PAIR_COUNT - 1)
    assert client.get("/graph").json()["edge_count"] == 0
    assert client.get("/graph?min_pairs=1").json()["edge_count"] == 1


def test_an_answer_from_outside_the_lookback_says_so(api, client):
    """Widening to the whole retained history is a different answer to a
    different question. Reporting it as a 30-minute lookback would be a lie
    the caller cannot detect."""
    add_pair(api, LAPTOP, SLEEVE, offset=600, count=20)      # ten hours ago
    add_trending(api, LAPTOP, offset=600)

    body = client.get(f"/related-products/{LAPTOP}").json()
    assert body["source"] == "co_occurrence"
    assert body["window"] == "latest_available"
    assert body["as_of"] is not None, "the caller must see how old this is"
    assert [r["product_id"] for r in body["related_products"]] == [SLEEVE]

    add_pair(api, LAPTOP, HUB, offset=1, count=5)            # a minute ago
    fresh = client.get(f"/related-products/{LAPTOP}").json()
    assert fresh["window"] == "recent" and fresh["as_of"] is None
    assert fresh["lookback_minutes"] == config.PAIR_LOOKBACK_MINUTES
    assert [r["product_id"] for r in fresh["related_products"]] == [HUB]


def test_the_graph_uses_the_same_lookback_as_related_products(api, client):
    """It summed every window ever written, so a graph next to a 30-minute
    'trending' panel was quietly showing all-time affinity."""
    add_pair(api, LAPTOP, SLEEVE, offset=600, count=20)      # ten hours ago
    add_pair(api, PHONE, SLEEVE, offset=1200, count=20)     # twenty hours ago
    old = client.get("/graph").json()
    assert old["window"] == "latest_available" and old["as_of"] is not None
    # Anchored on the newest data, not unbounded: the twenty-hour-old pair is
    # outside the same 30-minute lookback and must not appear.
    assert {(e["source"], e["target"]) for e in old["edges"]} == {(LAPTOP, SLEEVE)}

    add_pair(api, PHONE, HUB, offset=1, count=20)            # a minute ago
    # The graph is cached for GRAPH_CACHE_SECONDS because it groups every pair
    # row in the lookback - over a million after a replayed month, 5-6 seconds.
    # New data is therefore visible only once that expires; clear it here
    # rather than sleeping a minute.
    api._CACHE.clear()
    recent = client.get("/graph").json()
    assert recent["window"] == "recent"
    assert {(e["source"], e["target"]) for e in recent["edges"]} == {(HUB, PHONE)}
    assert recent["lookback_minutes"] == config.PAIR_LOOKBACK_MINUTES

    wide = client.get("/graph?minutes=2000").json()
    assert wide["edge_count"] == 3, "a caller can still ask for a wider window"


def test_the_graph_is_cached_because_it_is_the_expensive_one(api, client):
    """Measured at 5-6 s on a replayed month (1.2M pair rows), which is over
    the dashboard's timeout - so the page showed "no pairs yet" for data it
    had. Recomputing that per request keeps a core busy for nobody."""
    add_pair(api, LAPTOP, SLEEVE, count=20)
    client.get("/graph")
    client.get("/graph")
    counters = client.get("/stats").json()["counters"]
    assert counters["cache_hits"] >= 1 and counters["cache_misses"] == 1
    # A different question is a different cache entry.
    client.get("/graph?limit=5")
    assert client.get("/stats").json()["counters"]["cache_misses"] == 2


# --------------------------------------------------- pipeline and telemetry
def test_throughput_series_is_per_window(api, client):
    add_trending(api, LAPTOP, offset=2, events=60)
    add_trending(api, PHONE, offset=2, events=60)
    add_trending(api, LAPTOP, offset=1, events=30)

    series = client.get("/throughput?windows=5").json()["series"]
    assert [s["events"] for s in series] == [120, 30]      # oldest first
    assert series[0]["events_per_second"] == 2.0           # 120 in 60 s
    assert series[0]["products"] == 2


def test_pipeline_reports_how_far_behind_the_last_write_was(api, client):
    # Two minutes back, not one: with a one-minute offset the write time
    # (window end + 4 s) lands in the FUTURE whenever the test starts in the
    # first four seconds of a minute, so staleness came out negative and this
    # test failed about one run in fifteen. Two minutes keeps the write in the
    # past at every second of the clock while staying inside the 120-second
    # window that counts as "ok".
    start = minute(2)
    api._db[config.COLL_TRENDING].insert_one({
        "window_start": start, "window_end": start + timedelta(minutes=1),
        "product_id": LAPTOP, "event_count": 1, "score": 1.0,
        "unique_users": 1,
        # written 4 seconds after that window closed
        "_updated_at": start + timedelta(minutes=1, seconds=4)})

    body = client.get("/pipeline").json()
    assert body["status"] == "ok"
    assert body["lag_seconds"] == pytest.approx(4.0, abs=0.5)
    assert body["staleness_seconds"] > 0
    assert body["trigger_interval"] == config.TRIGGER_INTERVAL


def test_metrics_expose_the_spark_readings_prometheus_scrapes(api, client):
    api._db["pipeline_metrics"].insert_one({
        "recorded_at": datetime.now(timezone.utc), "query": "product_pairs",
        "batch_id": 7, "input_rows": 1200, "input_rows_per_second": 120.0,
        "processed_rows_per_second": 400.0, "batch_duration_ms": 3000,
        "state_rows": [1000, 500], "kafka_lag": [240.0, 100.0]})
    add_trending(api, LAPTOP)

    text = client.get("/metrics").text
    assert 'affinity_input_rows_per_second{query="product_pairs"} 120.0' in text
    assert 'affinity_kafka_lag_offsets{query="product_pairs"} 240.0' in text
    assert 'affinity_state_rows{query="product_pairs"} 1500' in text
    assert "affinity_pipeline_staleness_seconds" in text
    assert "# TYPE affinity_requests_total counter" in text


def test_metrics_omit_readings_that_are_unknown(api, client):
    """Lag is None when the broker cannot be asked; a 0 would read as healthy."""
    api._db["pipeline_metrics"].insert_one({
        "recorded_at": datetime.now(timezone.utc), "query": "trending",
        "batch_duration_ms": 1000, "state_rows": [10], "kafka_lag": [None]})
    text = client.get("/metrics").text
    assert "affinity_kafka_lag_offsets" not in text
    assert "affinity_batch_duration_ms" in text


def test_stats_counts_each_collection(api, client):
    add_trending(api, LAPTOP)
    add_pair(api, LAPTOP, SLEEVE)
    api._db[config.COLL_DLQ].insert_one({"raw_value": "{bad",
                                         "invalid_reason": "not_json"})
    body = client.get("/stats").json()
    assert body["collections"][config.COLL_TRENDING] == 1
    assert body["collections"][config.COLL_PAIRS] == 2     # both directions
    assert body["collections"][config.COLL_DLQ] == 1
    assert body["database"] == config.MONGO_DB


def test_a_database_failure_is_a_503_not_a_500(api, client, monkeypatch):
    from pymongo.errors import PyMongoError

    def broken(*_args, **_kwargs):
        raise PyMongoError("connection lost")

    monkeypatch.setattr(api._db[config.COLL_TRENDING], "aggregate", broken)
    add_trending(api, LAPTOP)
    assert client.get("/trending").status_code == 503
    assert client.get("/stats").json()["counters"]["errors_total"] >= 1


def test_every_documented_endpoint_answers(client):
    """The OpenAPI schema and the running app must not drift apart."""
    paths = client.get("/openapi.json").json()["paths"]
    assert set(paths) >= {"/health", "/trending", "/related-products/{product_id}",
                          "/graph", "/throughput", "/pipeline", "/stats",
                          "/metrics"}
    for path in paths:
        url = path.replace("{product_id}", str(LAPTOP))
        assert client.get(url).status_code in (200, 404), url
