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
    start = minute(1)
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
