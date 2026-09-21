"""
The real-data path: sessions, event mapping, replay timing, evaluation maths.

No dataset needed - every test builds the few rows it needs. The parts that
touch Kaggle, Kafka or Mongo live in the scripts; everything that decides what
a number MEANS is in src/data, and is checked here.

    pytest tests/test_data.py
"""

import json

import pytest

from src.common import catalog
from src.data import evaluation as ev
from src.data import retailrocket as rr

DAY = 86400.0


def rows(*triples):
    """Dataset rows as csv.DictReader would hand them over: all strings."""
    return [{"timestamp": str(int(at * 1000)), "visitorid": str(visitor),
             "event": event, "itemid": str(item), "transactionid": ""}
            for at, visitor, event, item in triples]


def events(*quads):
    return [rr.Event(at, visitor, item, kind) for at, visitor, item, kind in quads]


# ------------------------------------------------------------------ parsing
def test_event_types_map_onto_the_pipelines_own_names():
    parsed = list(rr.parse_rows(rows(
        (1.0, 7, "view", 100),
        (2.0, 7, "addtocart", 100),
        (3.0, 7, "transaction", 100))))
    assert [e.event_type for e in parsed] == ["view", "add_to_cart", "purchase"]
    # The weights the pipeline applies only work if the names line up.
    from src.common import config
    assert all(e.event_type in config.EVENT_WEIGHTS for e in parsed)


def test_timestamps_are_milliseconds():
    (event,) = list(rr.parse_rows(rows((1_430_622_000.0, 1, "view", 5))))
    assert event.at == pytest.approx(1_430_622_000.0)


def test_unusable_rows_are_dropped_not_raised():
    """A 2.7M-row public dataset with a few bad lines must not stop a replay."""
    parsed = list(rr.parse_rows(rows(
        (1.0, 1, "view", 5),
        (2.0, 1, "wishlist", 5),          # an event type this pipeline has no weight for
    ) + [{"timestamp": "oops", "visitorid": "1", "event": "view",
          "itemid": "5", "transactionid": ""},
         {"visitorid": "1", "event": "view", "itemid": "5"}]))
    assert len(parsed) == 1


# ------------------------------------------------------------------ sessions
def test_a_visitor_returning_later_is_a_new_visit():
    """Two products seen three weeks apart are not 'viewed together'."""
    visits = list(rr.sessionise(events(
        (0.0, 7, 100, "view"),
        (60.0, 7, 200, "view"),           # same visit
        (21 * DAY, 7, 300, "view"))))     # three weeks later
    assert [v.items for v in visits] == [[100, 200], [300]]
    assert visits[0].session != visits[1].session


def test_the_gap_is_what_cuts_a_visit():
    close = list(rr.sessionise(events((0.0, 1, 10, "view"),
                                      (29 * 60.0, 1, 20, "view"))))
    far = list(rr.sessionise(events((0.0, 1, 10, "view"),
                                    (31 * 60.0, 1, 20, "view"))))
    assert len(close) == 1 and len(far) == 2


def test_two_visitors_never_share_a_visit():
    visits = list(rr.sessionise(rr.sort_events(events(
        (0.0, 1, 10, "view"), (1.0, 2, 20, "view"), (2.0, 1, 30, "view")))))
    assert {v.visitor for v in visits} == {1, 2}
    assert all(len({e.visitor for e in v.events}) == 1 for v in visits)


def test_session_ids_are_stable_across_runs():
    """A replay has to be repeatable, or its event ids stop de-duplicating."""
    first = list(rr.sessionise(events((5.0, 3, 10, "view"), (6.0, 3, 20, "view"))))
    again = list(rr.sessionise(events((5.0, 3, 10, "view"), (6.0, 3, 20, "view"))))
    assert first[0].session == again[0].session
    assert first[0].session.startswith("rr-")


def test_items_are_distinct_and_in_view_order():
    visit = next(rr.sessionise(events(
        (0.0, 1, 10, "view"), (1.0, 1, 20, "view"),
        (2.0, 1, 10, "add_to_cart"), (3.0, 1, 30, "view"))))
    assert visit.items == [10, 20, 30]


# -------------------------------------------------------------------- clock
def test_replayed_events_land_now_not_in_2015():
    """2015 windows would fall outside every lookback the API has."""
    clock = rr.Clock(first_event_at=1_430_622_000.0, starts_at=1_700_000_000.0,
                     speedup=100.0)
    assert clock.at(1_430_622_000.0) == 1_700_000_000.0
    assert clock.at(1_430_622_100.0) == 1_700_000_001.0     # 100s / 100x


def test_compression_keeps_order_and_relative_spacing():
    clock = rr.Clock(0.0, 1_000.0, 60.0)
    times = [clock.at(t) for t in (0.0, 30.0, 90.0)]
    assert times == sorted(times)
    assert (times[2] - times[1]) == pytest.approx(2 * (times[1] - times[0]))


def test_a_speedup_of_zero_is_rejected():
    with pytest.raises(ValueError):
        rr.Clock(0.0, 0.0, 0.0)


def test_wire_events_are_what_the_parser_expects():
    from src.streaming import transforms as T

    event = rr.Event(1_430_622_000.0, 7, 100, "add_to_cart")
    wire = rr.to_wire(event, "rr-abc", rr.Clock(1_430_622_000.0, 1_700_000_000.0,
                                                1.0), 2)
    assert wire == {"event_id": "rr-abc-2", "session_id": "rr-abc",
                    "user_id": 7, "product_id": 100,
                    "timestamp": 1_700_000_000.0, "schema_version": 2,
                    "action": "add_to_cart", "channel": "replay"}
    # Every field the streaming schema reads must be present and the right type.
    fields = {f.name for f in T.EVENT_SCHEMA.fields}
    assert set(wire) <= fields, sorted(set(wire) - fields)


def test_replaying_the_same_slice_twice_gives_the_same_event_ids():
    clock = rr.Clock(0.0, 0.0, 1.0)
    visit = next(rr.sessionise(events((0.0, 1, 10, "view"), (1.0, 1, 20, "view"))))
    ids = [rr.to_wire(e, visit.session, clock, i)["event_id"]
           for i, e in enumerate(visit.events)]
    assert ids == [f"{visit.session}-0", f"{visit.session}-1"]
    assert len(set(ids)) == 2


# ----------------------------------------------------------------- catalogue
def test_catalog_built_from_real_items_loads_into_the_stack(tmp_path):
    products = rr.build_catalog([5, 7, 9], {5: 100, 7: 100})
    assert [p["category"] for p in products] == ["cat-100", "cat-100",
                                                 "uncategorised"]
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(products), encoding="utf-8")

    loaded = catalog.load_file(str(path))
    assert loaded == products
    assert all(p["price"] is None for p in loaded)


def test_a_broken_catalogue_fails_at_load_not_in_the_api(tmp_path):
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps([{"id": 1}]), encoding="utf-8")
    with pytest.raises(ValueError, match="missing"):
        catalog.load_file(str(path))

    path.write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="non-empty"):
        catalog.load_file(str(path))


def test_the_demo_catalogue_is_the_default():
    assert catalog.is_demo()
    assert catalog.PRODUCTS is catalog.DEMO_PRODUCTS


# ---------------------------------------------------------------- evaluation
def visits(*item_lists):
    made = []
    for n, items in enumerate(item_lists):
        made.append(rr.Visit(f"s{n}", n,
                             [rr.Event(float(i), n, item, "view")
                              for i, item in enumerate(items)]))
    return made


def test_a_visit_with_one_product_is_not_a_test_case():
    assert ev.test_cases(visits([10])) == []
    assert len(ev.test_cases(visits([10, 20]))) == 1


def test_the_case_asks_what_came_next():
    (case,) = ev.test_cases(visits([10, 20, 30]))
    assert (case.query, case.target, case.later) == (10, 20, (20, 30))


def test_bestsellers_are_the_most_viewed_and_ties_are_stable():
    assert ev.bestsellers({1: 5, 2: 9, 3: 9, 4: 1}, 2) == [2, 3]
    assert ev.bestsellers({3: 9, 2: 9, 1: 5}, 3) == [2, 3, 1]


def test_hit_rate_counts_the_next_product_only():
    cases = ev.test_cases(visits([10, 20, 30]))
    assert ev.evaluate("right", cases, lambda _q: [20]).hit_rate == 1.0
    # 30 was viewed later in the visit, but it is not what came next.
    later = ev.evaluate("later", cases, lambda _q: [30])
    assert later.hit_rate == 0.0 and later.any_hit_rate == 1.0


def test_the_query_product_is_dropped_before_the_top_k_is_taken():
    """Recommending the product back is never a hit, and never costs a slot."""
    cases = ev.test_cases(visits([10, 20]))
    assert ev.evaluate("echo", cases, lambda q: [q], k=10).hit_rate == 0.0
    assert ev.evaluate("echo", cases, lambda q: [q], k=10).coverage == 0.0
    assert ev.evaluate("echo", cases, lambda q: [q, 20], k=1).hit_rate == 1.0


def test_only_the_top_k_count():
    cases = ev.test_cases(visits([10, 99]))
    assert ev.evaluate("k", cases, lambda _q: [1, 2, 3, 99], k=3).hit_rate == 0.0
    assert ev.evaluate("k", cases, lambda _q: [1, 2, 3, 99], k=4).hit_rate == 1.0


def test_an_empty_answer_is_a_miss_and_lowers_coverage():
    """A model that answers 5% of queries perfectly is not a good model."""
    cases = ev.test_cases(visits([10, 20], [30, 40]))
    result = ev.evaluate("half", cases,
                         lambda q: [20] if q == 10 else [])
    assert (result.hit_rate, result.coverage) == (0.5, 0.5)


def test_a_deliberately_wrong_model_scores_below_the_baseline():
    """The harness has to be able to say 'worse', or it proves nothing."""
    cases = ev.test_cases(visits([10, 20], [10, 20], [30, 20]))
    bestseller = ev.evaluate("bestsellers", cases, lambda _q: [20])
    reversed_model = ev.evaluate("wrong", cases, lambda _q: [999])
    assert reversed_model.hit_rate < bestseller.hit_rate
    assert ev.lift(reversed_model, bestseller) == 0.0


def test_lift_is_none_when_the_baseline_scores_nothing():
    cases = ev.test_cases(visits([10, 20]))
    baseline = ev.evaluate("zero", cases, lambda _q: [999])
    assert ev.lift(ev.evaluate("model", cases, lambda _q: [20]), baseline) is None


def load_script(name):
    """Import one of the scripts/ files by path, the way the CLI runs it."""
    import importlib.util
    import pathlib

    path = (pathlib.Path(__file__).resolve().parents[1] / "scripts" / name)
    spec = importlib.util.spec_from_file_location(name.replace(".py", "_script"),
                                                  str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, _topic, key=None, value=None):
        self.sent.append(value)

    def flush(self):
        pass

    def close(self):
        pass


def test_the_replay_closes_its_own_last_windows():
    """Spark's watermark only moves when newer events arrive. Once a replay
    stops it is the only source of event time, so without these the windows
    holding its last minutes never close - on a five-minute slice that is
    most of the last two minutes of data."""
    replay = load_script("replay.py")
    producer = FakeProducer()
    replay.flush_windows(producer, after=1_000.0)

    stamps = [e["timestamp"] for e in producer.sent]
    assert len(producer.sent) == replay.FLUSH_MINUTES
    assert stamps == sorted(stamps) and min(stamps) > 1_000.0
    assert max(stamps) - 1_000.0 >= replay.FLUSH_MINUTES * 60


def test_the_watermark_events_cannot_invent_a_pair():
    """They must move time forward and change nothing else."""
    from src.streaming import transforms as T

    replay = load_script("replay.py")
    producer = FakeProducer()
    replay.flush_windows(producer, after=0.0)

    sessions = [e["session_id"] for e in producer.sent]
    assert len(set(sessions)) == len(sessions), "one session each, so none can pair"
    assert {e["product_id"] for e in producer.sent} == {replay.FLUSH_PRODUCT}
    assert replay.FLUSH_PRODUCT < 0, "a reserved id no dataset item can collide with"
    fields = {f.name for f in T.EVENT_SCHEMA.fields}
    assert all(set(e) <= fields for e in producer.sent)


def test_progress_reports_the_dataset_day_not_today():
    """The wire timestamp is already on the replay clock, so reading the day
    back out of it would always print today."""
    replay = load_script("replay.py")
    clock = rr.Clock(1_430_622_000.0, 1_700_000_000.0, 100.0)
    visit = next(rr.sessionise(events((1_430_622_000.0, 1, 10, "view"),
                                      (1_430_622_030.0, 1, 20, "view"))))
    rows = replay.wire_events([visit], clock)
    assert [row[3] for row in rows] == [1_430_622_000.0, 1_430_622_030.0]
    assert replay.day_of(rows[0][3]) == "2015-05-03"


def test_the_evaluation_runs_end_to_end_against_a_pair_table(tmp_path,
                                                             monkeypatch):
    """The whole script: split a file, query the pair table, write a report.

    Everything except MongoDB is real - the same argument parsing, the same
    aggregation, the same report. A renamed field in `product_pairs` fails
    here instead of producing a quietly zero hit-rate.
    """
    import csv
    import importlib
    import sys
    from datetime import datetime, timedelta, timezone

    import mongomock

    from src.common import config, mongo

    # Two training days of traffic, then a test day where every visit goes
    # 500 -> 600, which is exactly the pair the "pipeline" knows about.
    day = 86_400
    start = 1_430_622_000
    rows = []
    for visitor in range(200):
        for offset, item in ((0, 500), (30, 600)):
            rows.append({"timestamp": (start + visitor * 60 + offset) * 1000,
                         "visitorid": visitor, "event": "view",
                         "itemid": item, "transactionid": ""})
    for visitor in range(1000, 1100):                    # the test day
        for offset, item in ((0, 500), (30, 600)):
            rows.append({"timestamp": (start + 2 * day + visitor + offset) * 1000,
                         "visitorid": visitor, "event": "view",
                         "itemid": item, "transactionid": ""})
    events_csv = tmp_path / "events.csv"
    with open(events_csv, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=rr.EVENT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    client = mongomock.MongoClient()
    db = client[config.MONGO_DB]
    window = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    for first, second in ((500, 600), (600, 500)):
        db[config.COLL_PAIRS].insert_one({
            "window_start": window - timedelta(minutes=1),
            "window_end": window, "product_id": first,
            "related_product_id": second, "pair_count": 40,
            "affinity": 40.0, "unique_users": 20,
            "_updated_at": datetime.now(timezone.utc)})
    monkeypatch.setattr(mongo, "client", lambda _uri: client)

    sys.path.insert(0, str(tmp_path))                    # for the import below
    spec = importlib.util.spec_from_file_location(
        "evaluate_script",
        str(__import__("pathlib").Path(__file__).resolve().parents[1]
            / "scripts" / "evaluate.py"))
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)

    report = tmp_path / "EVALUATION.md"
    monkeypatch.setattr(script, "REPORT", report)
    monkeypatch.setattr(script, "RESULTS", tmp_path / "results")
    monkeypatch.setattr(sys, "argv", [
        "evaluate.py", "--events", str(events_csv), "--train-days", "2",
        "--test-days", "1", "--source", "mongo", "--k", "10"])

    assert script.main() == 0

    summary = json.loads((tmp_path / "results" / "evaluation.json")
                         .read_text(encoding="utf-8"))
    assert summary["cases"] == 100
    # The pair table says 500 -> 600, and that is what every test visit did.
    assert summary["pipeline"]["hit_rate"] == 1.0
    assert summary["pipeline"]["coverage"] == 1.0
    # The bestseller list contains 600 too, so this case cannot separate them -
    # what matters here is that both were scored on the same cases.
    assert summary["baseline"]["hit_rate"] > 0
    assert summary["lift"] is not None
    assert "hit-rate@10" in report.read_text(encoding="utf-8")


def test_an_out_of_order_export_does_not_truncate_the_split(tmp_path):
    """RetailRocket's events.csv is not in time order - its first row is
    2015-06-02 while its earliest event is 2015-05-03. Stopping at the first
    row past the window (which looks safe on a sorted file) silently threw
    away the rest of the read, and with a 30-day window found no test traffic
    at all."""
    import csv

    script = load_script("evaluate.py")
    day = 86_400
    start = 1_430_622_000
    rows = [
        # A row from far in the future, first in the file - the shape that
        # broke it.
        {"timestamp": (start + 90 * day) * 1000, "visitorid": 1,
         "event": "view", "itemid": 999, "transactionid": ""},
    ]
    for visitor in range(40):                       # training days
        for offset, item in ((0, 500), (30, 600)):
            rows.append({"timestamp": (start + visitor * 60 + offset) * 1000,
                         "visitorid": 100 + visitor, "event": "view",
                         "itemid": item, "transactionid": ""})
    for visitor in range(40):                       # the test day, later
        for offset, item in ((0, 500), (30, 600)):
            rows.append({"timestamp": (start + 2 * day + visitor + offset) * 1000,
                         "visitorid": 200 + visitor, "event": "view",
                         "itemid": item, "transactionid": ""})

    events_csv = tmp_path / "events.csv"
    with open(events_csv, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=rr.EVENT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    train_views, train_events, visits, begin, (first, newest) = script.split(
        str(events_csv), train_days=2, test_days=1, start=None)
    assert visits, "the test window must survive an out-of-order first row"
    assert train_events == 80
    assert newest > first, "the reported span covers the whole file"


def test_the_mongo_source_does_not_decay_with_the_clock(monkeypatch):
    """A replay's windows age out of the lookback within the hour. If the
    pair-table reader answered only from the recent window, the same data
    would score 7.9% at one moment and 0% twenty minutes later - a number
    that depends on when you ran it is not a measurement."""
    from datetime import datetime, timedelta, timezone

    import mongomock

    from src.common import config, mongo

    script = load_script("evaluate.py")
    client = mongomock.MongoClient()
    db = client[config.MONGO_DB]
    old_window = datetime.now(timezone.utc) - timedelta(hours=5)
    for first, second in ((10, 20), (20, 10)):
        db[config.COLL_PAIRS].insert_one({
            "window_start": old_window,
            "window_end": old_window + timedelta(minutes=1),
            "product_id": first, "related_product_id": second,
            "pair_count": 50, "affinity": 150.0, "unique_users": 9,
            "_updated_at": old_window})
    monkeypatch.setattr(mongo, "client", lambda _uri: client)

    widening = script.MongoRecommender(10, config.PAIR_LOOKBACK_MINUTES)
    assert widening(10) == [20]
    assert widening.sources["co_occurrence_retained"] == 1

    strict = script.MongoRecommender(10, config.PAIR_LOOKBACK_MINUTES,
                                     widen=False)
    assert strict(10) == [], "--strict-lookback still answers only from recent"
    assert strict.sources["no_pairs"] == 1


def test_an_api_that_knows_no_products_is_an_error_not_a_zero(tmp_path,
                                                              monkeypatch,
                                                              caplog):
    """Serving the demo catalogue makes every real item a 404. Reporting
    0.000% for that would be a measurement of nothing."""
    import csv
    import sys

    script = load_script("evaluate.py")

    day = 86_400
    start = 1_430_622_000
    rows = []
    for visitor in range(60):
        for offset, item in ((0, 500), (30, 600)):
            rows.append({"timestamp": (start + visitor * 60 + offset) * 1000,
                         "visitorid": visitor, "event": "view",
                         "itemid": item, "transactionid": ""})
    for visitor in range(1000, 1040):
        for offset, item in ((0, 500), (30, 600)):
            rows.append({"timestamp": (start + 2 * day + visitor + offset) * 1000,
                         "visitorid": visitor, "event": "view",
                         "itemid": item, "transactionid": ""})
    events_csv = tmp_path / "events.csv"
    with open(events_csv, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=rr.EVENT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    class NotFoundApi:
        """What /related-products does for a product it has no catalogue for."""

        def __init__(self, *_args, **_kwargs):
            self.sources = __import__("collections").Counter()

        def __call__(self, _product_id):
            self.sources["unknown_product"] += 1
            return []

    monkeypatch.setattr(script, "ApiRecommender", NotFoundApi)
    monkeypatch.setattr(script, "REPORT", tmp_path / "EVALUATION.md")
    monkeypatch.setattr(script, "RESULTS", tmp_path / "results")
    monkeypatch.setattr(sys, "argv", [
        "evaluate.py", "--events", str(events_csv), "--train-days", "2",
        "--test-days", "1", "--source", "api"])

    with caplog.at_level("ERROR"):
        assert script.main() == 1, "a 404 for every product is not a result"
    assert not (tmp_path / "EVALUATION.md").exists(), "no report from a non-result"
    assert "CATALOG_FILE" in caplog.text, "the message has to name the fix"


def test_the_scripts_exist_and_say_how_they_are_run():
    """Each stage names the next one, so the sequence is discoverable."""
    import pathlib
    root = pathlib.Path(__file__).resolve().parents[1]
    fetch = (root / "scripts" / "fetch_dataset.py").read_text(encoding="utf-8")
    replay = (root / "scripts" / "replay.py").read_text(encoding="utf-8")
    evaluate = (root / "scripts" / "evaluate.py").read_text(encoding="utf-8")
    assert "kaggle.json" in fetch and "replay.py" in fetch
    assert "--days" in replay and "CATALOG_FILE" in replay
    assert "--train-days" in evaluate and "bestseller" in evaluate.lower()


# --- the replay manifest -----------------------------------------------------
#
# Nothing in the pipeline's own rows says which days it was fed: they carry
# replay-clock timestamps, not dataset dates. So the replay writes down what it
# sent and the evaluation checks the answer against it.

def test_a_training_window_that_matches_the_replay_passes():
    script = load_script("evaluate.py")
    run = {"start_day": "2015-05-03", "days": 30.0, "events": 617_109}
    assert script.check_against_replay(run, "2015-05-03", 30.0) is None


def test_testing_on_days_the_pipeline_was_trained_on_is_refused():
    """The measured cost of missing this: 35.1% against a truthful 17.4%.

    A 30-day replay followed by `--train-days 7 --test-days 7` puts days 7-14
    in the test set AND in the pipeline, so the model is asked about visits it
    has already seen.
    """
    script = load_script("evaluate.py")
    run = {"start_day": "2015-05-03", "days": 30.0, "events": 617_109}
    problem = script.check_against_replay(run, "2015-05-03", 7.0)
    assert problem and "30" in problem and "7 days" in problem
    assert "--allow-window-mismatch" in problem, "say how to override it"

    # A different start day is the same mistake seen from the other side.
    assert script.check_against_replay(run, "2015-06-02", 30.0) is not None


def test_no_manifest_means_no_check():
    """Older stacks, and anyone driving the pipeline by hand, have no record.
    Refusing to measure at all would be worse than measuring unguarded."""
    script = load_script("evaluate.py")
    assert script.check_against_replay(None, "2015-05-03", 7.0) is None


def test_the_replay_records_what_it_sent(monkeypatch):
    import mongomock

    from src.common import config, mongo

    replay = load_script("replay.py")
    client = mongomock.MongoClient()
    monkeypatch.setattr(mongo, "client", lambda _uri: client)
    replay.record_run(start=1_430_622_000.0, days=30.0, events=617_109,
                      speedup=2016.0)

    run = client[config.MONGO_DB][replay.REPLAY_RUNS].find_one()
    assert run["start_day"] == "2015-05-03" and run["days"] == 30.0
    assert run["events"] == 617_109 and run["dataset"] == "retailrocket"
    # The evaluation reads this document, so the two have to agree on shape.
    script = load_script("evaluate.py")
    assert script.check_against_replay(run, "2015-05-03", 30.0) is None


def test_a_leaking_evaluation_stops_before_it_prints_a_number(tmp_path,
                                                              monkeypatch,
                                                              caplog):
    """End to end: a mismatched manifest exits 2 and writes no report."""
    import csv
    import sys
    from datetime import datetime, timezone

    import mongomock

    from src.common import config, mongo

    day = 86_400
    start = 1_430_622_000
    rows = []
    for visitor in range(200):
        for offset, item in ((0, 500), (30, 600)):
            rows.append({"timestamp": (start + visitor * 60 + offset) * 1000,
                         "visitorid": visitor, "event": "view",
                         "itemid": item, "transactionid": ""})
    for visitor in range(1000, 1100):
        for offset, item in ((0, 500), (30, 600)):
            rows.append({"timestamp": (start + 2 * day + visitor + offset) * 1000,
                         "visitorid": visitor, "event": "view",
                         "itemid": item, "transactionid": ""})
    events_csv = tmp_path / "events.csv"
    with open(events_csv, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=rr.EVENT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    client = mongomock.MongoClient()
    client[config.MONGO_DB]["replay_runs"].insert_one({
        "dataset": "retailrocket", "start_day": "2015-05-03", "days": 10.0,
        "events": 1_000, "speedup": 2016.0,
        "finished_at": datetime.now(timezone.utc)})
    monkeypatch.setattr(mongo, "client", lambda _uri: client)

    script = load_script("evaluate.py")
    monkeypatch.setattr(script, "REPORT", tmp_path / "EVALUATION.md")
    monkeypatch.setattr(script, "RESULTS", tmp_path / "results")
    argv = ["evaluate.py", "--events", str(events_csv), "--train-days", "2",
            "--test-days", "1", "--source", "mongo"]
    monkeypatch.setattr(sys, "argv", argv)

    with caplog.at_level("ERROR"):
        assert script.main() == 2, "a 10-day replay cannot answer a 2-day split"
    assert not (tmp_path / "EVALUATION.md").exists()
    assert "10" in caplog.text

    # ...and the override still measures, because sometimes you know better.
    monkeypatch.setattr(sys, "argv", argv + ["--allow-window-mismatch"])
    assert script.main() == 0
    assert (tmp_path / "EVALUATION.md").exists()
