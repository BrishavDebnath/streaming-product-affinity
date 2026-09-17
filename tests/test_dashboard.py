"""
Dashboard tests: run the real Streamlit script against a fake API.

Streamlit's AppTest executes `src/ui/dashboard.py` exactly as the browser
would, so a KeyError from a renamed API field fails here instead of on
screen. The API underneath is the real FastAPI app on mongomock, so a change
to either side that breaks the other is caught.

    pytest tests/test_dashboard.py
"""

import importlib
import os
from datetime import datetime, timedelta, timezone

import mongomock
import pytest
from fastapi.testclient import TestClient
from streamlit.testing.v1 import AppTest

from src.common import config

DASHBOARD = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "src", "ui", "dashboard.py")
LAPTOP, XPS, SLEEVE, SHOE = 9001, 9002, 9003, 9011


@pytest.fixture()
def stack(monkeypatch):
    """The dashboard, wired to the real API over a fake MongoDB."""
    import src.api.main as main

    importlib.reload(main)
    main._client = mongomock.MongoClient()
    main._db = main._client[config.MONGO_DB]
    client = TestClient(main.app)

    class Response:
        def __init__(self, inner):
            self._inner = inner

        def raise_for_status(self):
            self._inner.raise_for_status()

        def json(self):
            return self._inner.json()

    import requests

    def fake_get(url, timeout=None):
        return Response(client.get(url.replace(config.API_BASE_URL, "")))

    monkeypatch.setattr(requests, "get", fake_get)
    # No Kafka in a unit test: the dashboard only builds a producer when a
    # product button is clicked, and those clicks are not part of these tests.
    return main


def minute(offset):
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    return now - timedelta(minutes=offset)


def fill(api, pairs=((LAPTOP, SLEEVE, 30), (LAPTOP, XPS, 20), (LAPTOP, SHOE, 5))):
    start = minute(1)
    written = datetime.now(timezone.utc)
    for product, events in ((LAPTOP, 80), (XPS, 60), (SLEEVE, 40), (SHOE, 20)):
        api._db[config.COLL_TRENDING].insert_one({
            "window_start": start, "window_end": start + timedelta(minutes=1),
            "product_id": product, "event_count": events,
            "score": float(events), "unique_users": 5, "_updated_at": written})
    for a, b, count in pairs:
        for first, second in ((a, b), (b, a)):
            api._db[config.COLL_PAIRS].insert_one({
                "window_start": start, "window_end": start + timedelta(minutes=1),
                "product_id": first, "related_product_id": second,
                "pair_count": count, "affinity": float(count),
                "unique_users": 2, "_updated_at": written})


def run():
    return AppTest.from_file(DASHBOARD, default_timeout=60).run()


def metrics(app):
    return {m.label: m.value for m in app.metric}


def test_dashboard_renders_with_data(stack):
    fill(stack)
    app = run()

    assert not app.exception
    assert app.title[0].value == "Streaming Product Affinity Pipeline"
    labels = {m.label for m in app.metric}
    assert {"Processing delay", "Last update", "Events in the last full minute",
            "Trending rows", "Products shown", "Lines drawn"} <= labels
    assert metrics(app)["Trending rows"] == "4"
    headers = [h.value for h in app.subheader]
    assert "Products viewed together" in headers and "Trending now" in headers


def test_dashboard_reads_every_field_the_api_returns(stack):
    """A renamed API field must fail here, not in the browser."""
    fill(stack)
    app = run()
    assert not app.exception
    assert not app.error                      # the API-unreachable banner
    tables = [t.value for t in app.dataframe]
    assert tables, "trending and related products should both render"
    columns = {c for table in tables for c in table.columns}
    assert "Product" in columns


def test_graph_marks_cross_category_links(stack):
    fill(stack)
    app = run()
    dot = app.get("graphviz_chart")[0].proto.spec
    assert "style=solid" in dot and "style=dashed" in dot
    assert metrics(app)["Cross-category lines"] == "1"       # laptop + shoe


def test_showing_more_lines_shows_the_weaker_pairs(stack):
    fill(stack, pairs=[(LAPTOP, XPS, 40), (LAPTOP, SLEEVE, 30),
                       (LAPTOP, SHOE, 5)])
    app = run()
    app.slider[0].set_value(10).run()
    assert metrics(app)["Lines drawn"] == "3"
    app.slider[0].set_value(20).run()
    assert not app.exception


def test_empty_pipeline_explains_itself(stack):
    app = run()
    assert not app.exception
    text = " ".join(i.value for i in app.info)
    assert "No data yet" in text or "No product pairs yet" in text


def test_dashboard_says_so_when_the_api_is_down(stack, monkeypatch):
    import requests

    def refuse(*_args, **_kwargs):
        raise requests.RequestException("connection refused")

    monkeypatch.setattr(requests, "get", refuse)
    app = run()
    assert not app.exception
    assert any("not reachable" in e.value for e in app.error)
