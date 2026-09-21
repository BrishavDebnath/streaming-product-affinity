"""
Dashboard tests: run the real Streamlit script against a fake API.

Streamlit's AppTest executes `src/ui/dashboard.py` exactly as the browser
would, so a KeyError from a renamed API field fails here instead of on
screen. The API underneath is the real FastAPI app on mongomock, so a change
to either side that breaks the other is caught.

    pytest tests/test_dashboard.py
"""

import importlib
import json
import os
import re
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
        """What `requests` would hand back, including its error type: the
        dashboard catches requests.RequestException, so a fake that raises
        httpx's error instead would make a handled 404 look like a crash."""

        def __init__(self, inner):
            self._inner = inner

        def raise_for_status(self):
            import requests as _requests

            if self._inner.status_code >= 400:
                raise _requests.HTTPError(
                    f"{self._inner.status_code} for {self._inner.url}")

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


def graph_of(app):
    """The drawn graph: the hover page's SVG when Graphviz is installed (as
    in the container), the DOT given to st.graphviz_chart when it is not."""
    frames = app.get("iframe")
    if frames:
        return frames[0].proto.srcdoc
    return app.get("graphviz_chart")[0].proto.spec


def node_fills(markup):
    """Fill colour of every product box, from either form of the graph."""
    if "<svg" in markup:
        return re.findall(r'class="node">\s*<title>[^<]*</title>\s*<path fill="([^"]+)"',
                          markup)
    return re.findall(r'^\s*n\d+ \[.*fillcolor="([^"]+)"', markup, re.M)


def dashed_lines(markup):
    """How many lines are dashed, from either form of the graph."""
    if "<svg" in markup:
        return len(re.findall(r'class="edge">(?:(?!</g>).)*stroke-dasharray',
                              markup, re.S))
    return markup.count("style=dashed")


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
    drawn = graph_of(app)
    # laptop + shoe: one of the three lines crosses categories, and it is the
    # one dashed line.
    assert dashed_lines(drawn) == 1
    assert metrics(app)["Lines crossing categories"] == "1 of 3"


@pytest.mark.skipif(not __import__("shutil").which("dot"),
                    reason="needs the Graphviz dot binary (the container image has it)")
def test_counts_appear_on_hover_not_on_the_lines(stack):
    """Thirty printed counts overlapped each other and the boxes. The count
    now appears in a small box when the pointer is on its line - and the box
    must describe that line, so the table is keyed by the edge's own id."""
    fill(stack)
    app = run()
    page = graph_of(app)
    assert "<svg" in page, "the hover page, not the plain chart"
    edges = re.findall(r'<g id="(e\d+)" class="edge">', page)
    assert len(edges) == 3
    table = json.loads(re.search(r"const EDGES = (\{.*?\});", page).group(1))
    assert sorted(table) == sorted(edges), "every line has an entry, none extra"
    assert sorted(e["n"] for e in table.values()) == [5, 20, 30]
    # No count is printed on a line any more: edges carry no text.
    assert not re.search(r'class="edge">(?:(?!</g>).)*<text', page, re.S)
    assert 'id="tip"' in page and "rgba(" in page, "a translucent box"


def test_a_product_name_cannot_break_out_of_the_hover_script():
    from src.ui import dashboard as ui

    graph = {"nodes": [{"id": 1, "name": "</script><b>x", "category": "a"},
                       {"id": 2, "name": "Item 2", "category": "a"}],
             "edges": [{"source": 1, "target": 2, "pair_count": 3,
                        "affinity": 1.0}]}
    page = ui.graph_page("<svg></svg>", graph)
    script = page.split("<script>", 1)[1]
    assert script.count("</script>") == 1, "only the real closing tag"


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


def test_dashboard_survives_a_real_catalogue(stack, monkeypatch):
    """A catalogue built from a real dataset has no prices and tens of
    thousands of items. Formatting a None price crashed the page, and a button
    pair per item would render for minutes."""
    from src.common import catalog as catalog_module
    from src.data import retailrocket as rr

    real = rr.build_catalog(range(9000, 14000), {9000: 1037, 9001: 1037})
    assert all(p["price"] is None for p in real)
    monkeypatch.setattr(catalog_module, "PRODUCTS", real)
    # What CATALOG_FILE being set does: the page must stop explaining the
    # demo generator's own behaviour as if it were a property of the data.
    monkeypatch.setattr(catalog_module, "CATALOG_FILE", "catalog.json")

    fill(stack)
    app = run()

    assert not app.exception, app.exception
    views = [b for b in app.button if b.label == "View"]
    assert len(views) == 12, f"{len(views)} products rendered buttons"
    captions = " ".join(c.value for c in app.caption)
    assert "of 5,000 catalogue items" in captions
    assert "Rs None" not in captions
    # The chooser offers what has data plus a bounded slice of the catalogue,
    # never all 5,000.
    options = app.selectbox[0].options
    assert 0 < len(options) <= 4 + 50
    assert "demo data does in about 15%" not in captions
    assert "same category" in captions


def test_the_page_says_when_it_is_showing_history_not_now(stack):
    """A finished replay leaves pairs outside the lookback. The page must not
    present that history as live activity."""
    fill(stack, pairs=[(LAPTOP, SLEEVE, 30)])
    # Move every row well outside the lookback, the way a finished replay
    # leaves them once the clock moves on.
    shift = timedelta(hours=10)
    for collection in (config.COLL_TRENDING, config.COLL_PAIRS):
        for row in list(stack._db[collection].find()):
            stack._db[collection].update_one(
                {"_id": row["_id"]},
                {"$set": {"window_start": row["window_start"] - shift,
                          "window_end": row["window_end"] - shift}})

    app = run()
    assert not app.exception
    captions = " ".join(c.value for c in app.caption)
    assert "most recent" in captions, captions


def test_the_graph_colours_the_categories_it_actually_has(stack, monkeypatch):
    """Every node used to come out grey on a real catalogue, under a legend
    advertising the demo's six categories - colours that appeared nowhere in
    the picture."""
    from src.common import catalog as catalog_module
    from src.data import retailrocket as rr
    from src.ui import dashboard as ui

    real = rr.build_catalog([LAPTOP, XPS, SLEEVE, SHOE],
                            {LAPTOP: 7, XPS: 7, SLEEVE: 9, SHOE: 11})
    monkeypatch.setattr(catalog_module, "PRODUCTS", real)
    monkeypatch.setattr(catalog_module, "_BY_ID", {p["id"]: p for p in real})
    monkeypatch.setattr(catalog_module, "CATALOG_FILE", "catalog.json")
    monkeypatch.setattr(catalog_module, "AFFINITY",
                        {p["category"]: [p["category"]] for p in real})

    fill(stack)
    # Importing the page above ran it once against an empty database, and the
    # graph is cached for GRAPH_CACHE_SECONDS - so without this the run below
    # would be served the empty answer.
    stack._CACHE.clear()
    app = run()
    assert not app.exception, app.exception

    used = node_fills(graph_of(app))
    assert used, "no nodes drawn"
    assert set(used) != {ui.CATEGORY_COLOUR["unknown"]}, "every node grey again"
    # Same category, same colour; different categories, different colours.
    colours = ui.colour_map([p["category"] for p in real])
    assert colours["cat-7"] != colours["cat-9"] != colours["cat-11"]
    assert len(set(colours.values())) == len({p["category"] for p in real})

    markdown = " ".join(m.value for m in app.markdown)
    assert "Laptops" not in markdown and "Footwear" not in markdown
    assert "cat-7" in markdown, "the legend must name the graph's categories"


def test_no_two_categories_ever_share_a_colour(stack):
    """A 30-day RetailRocket replay put 31 categories on one graph. Wrapping
    round a 12-colour palette gave unrelated items the same colour - three
    gold boxes, two of them joined by dashed "different category" lines - so
    the picture contradicted itself. Up to the palette, every category gets
    its own colour and the legend names all of them; past it, no category
    gets a colour and each box prints its category instead."""
    from src.ui import dashboard as ui

    few = [f"cat-{i}" for i in range(len(ui.PALETTE))]
    colours = ui.colour_map(few)
    assert len(set(colours.values())) == len(few), "one colour per category"
    assert not ui.labels_categories(colours)
    assert len(ui.PALETTE) <= ui.LEGEND_CATEGORIES, "every colour is named"

    many = [f"cat-{i}" for i in range(31)]
    colours = ui.colour_map(many)
    assert set(colours.values()) == {ui.NEUTRAL_COLOUR}
    assert ui.labels_categories(colours)

    graph = {"nodes": [{"id": i, "name": f"Item {i}", "category": c}
                       for i, c in enumerate(many)],
             "edges": [{"source": 0, "target": 1, "affinity": 2.0,
                        "pair_count": 5}]}
    dot = ui.build_dot(graph, lambda a, b: a == b, colours)
    assert 'label="Item 0\\ncat-0"' in dot, "the category moves into the label"
    assert "style=dashed" in dot, "the line still says: different categories"

    # The demo keeps its hand-picked colours and plain labels.
    demo = ui.colour_map(["laptop", "footwear"])
    assert demo == {"laptop": ui.CATEGORY_COLOUR["laptop"],
                    "footwear": ui.CATEGORY_COLOUR["footwear"]}
    assert not ui.labels_categories(demo)


def test_stale_windows_are_labelled_as_history(stack):
    """A finished replay leaves the newest window an hour behind. Calling it
    'the last full minute' reports a stopped pipeline as a running one."""
    fill(stack)
    shift = timedelta(minutes=45)
    for collection in (config.COLL_TRENDING, config.COLL_PAIRS):
        for row in list(stack._db[collection].find()):
            stack._db[collection].update_one(
                {"_id": row["_id"]},
                {"$set": {"window_start": row["window_start"] - shift,
                          "window_end": row["window_end"] - shift}})

    app = run()
    assert not app.exception
    labels = {m.label for m in app.metric}
    assert "Events in the newest full minute" in labels
    assert "Events in the last full minute" not in labels
    captions = " ".join(c.value for c in app.caption)
    assert "one-minute windows" in captions
    assert "still in progress" not in captions, "no window is in progress"
    assert "minutes ago" in captions
    info = " ".join(i.value for i in app.info)
    assert "No events in the last" in info, "trending must say why it is empty"


def test_the_numbers_on_the_page_are_the_numbers_in_the_database(stack):
    """Arithmetic, from first principles.

    Seed windows whose totals can be worked out by hand, then check the page
    against them: the per-minute event count, the rate, how many edges survive
    de-duplication, how many products that leaves, and the affinity summed
    across windows. If any aggregation drifts, these stop matching.
    """
    products = range(1, 30)
    pairs = {(101, 102): 90, (101, 103): 40, (102, 104): 25, (105, 106): 15}
    windows = 6
    start = minute(windows + 1)
    written = datetime.now(timezone.utc)
    for w in range(windows):
        opened = start + timedelta(minutes=w)
        for product in products:
            stack._db[config.COLL_TRENDING].insert_one({
                "window_start": opened, "window_end": opened + timedelta(minutes=1),
                "product_id": product, "event_count": 500 + product,
                "score": float(500 + product), "unique_users": 5,
                "_updated_at": written})
        for (a, b), count in pairs.items():
            for first, second in ((a, b), (b, a)):      # the sink mirrors pairs
                stack._db[config.COLL_PAIRS].insert_one({
                    "window_start": opened,
                    "window_end": opened + timedelta(minutes=1),
                    "product_id": first, "related_product_id": second,
                    "pair_count": count, "affinity": float(count * 3),
                    "unique_users": 2, "_updated_at": written})

    app = run()
    assert not app.exception, app.exception
    shown = metrics(app)

    per_window = sum(500 + p for p in products)                     # 14,935
    assert shown["Events in the last full minute"] == f"{per_window:,}"
    assert shown["Events per second"] == f"{per_window / 60:,.1f}"

    # Each pair was written twice (both directions) in each of six windows;
    # the graph must show one edge per pair, once.
    assert shown["Lines drawn"] == str(len(pairs))
    assert shown["Products shown"] == str(len({p for pair in pairs for p in pair}))

    strongest = [c.value for c in app.caption if " + " in c.value and "score" in c.value]
    expected = [f"score {count * 3 * windows}.0"
                for _, count in sorted(pairs.items(), key=lambda kv: -kv[1])]
    assert [line.split(" - ")[-1] for line in strongest] == expected


def test_dashboard_says_so_when_the_api_is_down(stack, monkeypatch):
    import requests

    def refuse(*_args, **_kwargs):
        raise requests.RequestException("connection refused")

    monkeypatch.setattr(requests, "get", refuse)
    app = run()
    assert not app.exception
    assert any("not reachable" in e.value for e in app.error)


def test_last_update_reads_like_a_clock():
    """A finished replay left the page saying '3649 s ago'."""
    from src.ui import dashboard as ui

    assert ui.ago(4.4) == "4 s ago"
    assert ui.ago(59) == "59 s ago"
    assert ui.ago(360) == "6 min ago"
    assert ui.ago(3649) == "1 h ago"
    assert ui.ago(3720) == "1 h 2 min ago"
    assert ui.ago(3 * 86400) == "3 days ago"
