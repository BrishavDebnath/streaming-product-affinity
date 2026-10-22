"""
The learned re-ranker: the feature function, the model guards, and the API's
`score_by=learned` path, all over a fake MongoDB.

    pytest tests/test_ranking.py
"""

import importlib
import json
from datetime import datetime, timedelta, timezone

import mongomock
import numpy as np
import pytest
from fastapi.testclient import TestClient

from src.common import catalog, config
from src.ranking import features
from src.ranking import model as ranking_model

LAPTOP, XPS, SLEEVE, HUB, PHONE = 9001, 9002, 9003, 9004, 9005
RUN = {"dataset": "retailrocket", "start_day": "2015-05-03", "days": 30.0,
       "finished_at": datetime.now(timezone.utc)}


def minute(offset):
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    return now - timedelta(minutes=offset)


def add_trending(db, product, offset=1, events=10, views=None, carts=0, purchases=0):
    start = minute(offset)
    row = {"window_start": start, "window_end": start + timedelta(minutes=1),
           "product_id": product, "event_count": events, "score": float(events),
           "unique_users": 1}
    if views is not None:
        row.update(views=views, carts=carts, purchases=purchases)
    db[config.COLL_TRENDING].insert_one(row)


def add_pair(db, a, b, offset=1, count=10):
    start = minute(offset)
    for first, second in ((a, b), (b, a)):
        db[config.COLL_PAIRS].insert_one({
            "window_start": start, "window_end": start + timedelta(minutes=1),
            "product_id": first, "related_product_id": second,
            "pair_count": count, "affinity": float(count), "unique_users": 1})


def category_of(pid):
    return (catalog.get(pid) or {}).get("category")


@pytest.fixture()
def db():
    return mongomock.MongoClient()[config.MONGO_DB]


@pytest.fixture()
def stocked(db):
    for pid, n in ((LAPTOP, 50), (XPS, 40), (SLEEVE, 30), (HUB, 20), (PHONE, 60)):
        add_trending(db, pid, events=n, views=n - 5, carts=3, purchases=1)
    add_pair(db, LAPTOP, SLEEVE, count=12)
    add_pair(db, LAPTOP, HUB, count=2)           # below MIN_PAIR_COUNT
    add_pair(db, PHONE, HUB, count=5)
    return db


def snapshot(db, embeddings=None):
    return features.Snapshot.from_mongo(db, features.anchored_cutoff(db),
                                        category_of=category_of, parent={},
                                        embeddings=embeddings)


# ----------------------------------------------------------- feature function
def test_candidates_put_co_occurrence_first_and_never_the_query(stocked):
    snap = snapshot(stocked)
    nbrs = features.neighbours_from_mongo(stocked, LAPTOP, features.anchored_cutoff(stocked))
    cands, matrix, origin = features.rows(LAPTOP, nbrs, snap)
    assert cands[:2] == [SLEEVE, HUB]
    assert LAPTOP not in cands
    assert origin[SLEEVE] == "co_occurrence" and origin[XPS] == "category"
    assert matrix.shape == (len(cands), len(features.FEATURE_NAMES))


def test_thin_pairs_are_candidates_but_flagged(stocked):
    snap = snapshot(stocked)
    nbrs = features.neighbours_from_mongo(stocked, LAPTOP, None)
    cands, matrix, _ = features.rows(LAPTOP, nbrs, snap)
    col = features.FEATURE_NAMES.index("passes_min_pairs")
    assert matrix[cands.index(SLEEVE), col] == 1.0
    assert matrix[cands.index(HUB), col] == 0.0     # 2 pairs: shown to the model only


def test_features_read_the_per_type_counts(stocked):
    snap = snapshot(stocked)
    assert snap.views[LAPTOP] == 45 and snap.carts[LAPTOP] == 3 and snap.buys[LAPTOP] == 1
    # Each co-view is stored twice; the total counts it once.
    assert snap.total_pairs == 12 + 2 + 5


def test_rows_written_before_the_split_count_every_event_as_a_view(db):
    add_trending(db, LAPTOP, events=7)
    assert snapshot(db).views[LAPTOP] == 7


def test_same_category_feature(stocked):
    snap = snapshot(stocked)
    cands, matrix, _ = features.rows(LAPTOP, {}, snap)
    col = features.FEATURE_NAMES.index("same_cat")
    assert matrix[cands.index(XPS), col] == 1.0      # both laptops
    assert matrix[cands.index(PHONE), col] == 0.0


def test_embeddings_find_the_nearest_item():
    emb = features.Embeddings(np.array([1, 2, 3]),
                              np.array([[1.0, 0.0], [0.9, 0.1], [0.0, 1.0]]))
    assert emb.similar(1, n=2) == [2, 3]
    assert emb.cosine(1, 2) > emb.cosine(1, 3)
    assert emb.similar(99) == [] and emb.cosine(1, 99) == 0.0


# ---------------------------------------------------------------- the guards
def write_model(path, names=features.FEATURE_NAMES, window=None):
    import lightgbm as lgb

    rng = np.random.default_rng(0)
    X = rng.random((200, len(features.FEATURE_NAMES)))
    y = (X[:, 0] > 0.5).astype(int)                 # learns "more pairs is better"
    model = lgb.LGBMRanker(objective="lambdarank", n_estimators=20, min_child_samples=5,
                           verbose=-1).fit(X, y, group=[20] * 10)
    path.mkdir(parents=True, exist_ok=True)
    model.booster_.save_model(str(path / "model.txt"))
    (path / "metadata.json").write_text(json.dumps(
        {"version": "test", "feature_names": list(names)}), encoding="utf-8")
    ids = np.array([LAPTOP, XPS, SLEEVE, HUB, PHONE])
    np.savez(path / "item2vec.npz", ids=ids, vectors=rng.random((5, 4)))
    (path / "item2vec.json").write_text(json.dumps(
        window or {"start_day": RUN["start_day"], "days": RUN["days"]}), encoding="utf-8")
    return path


def test_a_missing_model_is_reported_not_raised_later(tmp_path):
    with pytest.raises(ranking_model.ModelError, match="no trained model"):
        ranking_model.load(tmp_path)


def test_a_model_trained_on_other_features_is_refused(tmp_path):
    write_model(tmp_path, names=("pair_n", "something_else"))
    with pytest.raises(ranking_model.ModelError, match="retrain"):
        ranking_model.load(tmp_path)


def test_vectors_from_another_window_are_refused(tmp_path):
    ranker = ranking_model.load(write_model(tmp_path, window={"start_day": "2015-06-01",
                                                              "days": 30}))
    assert "Run scripts/train_ranker.py --embeddings-only" in ranker.check_window(RUN)
    assert "demo traffic" in ranker.check_window(None)


def test_matching_vectors_pass(tmp_path):
    assert ranking_model.load(write_model(tmp_path)).check_window(RUN) is None


# ---------------------------------------------------------------- the API path
@pytest.fixture()
def api(stocked, monkeypatch, tmp_path):
    import src.api.main as main

    importlib.reload(main)
    main._db = stocked
    monkeypatch.setattr(config, "RANKER_DIR", str(tmp_path / "ranker"))
    return main


def test_learned_without_a_model_answers_as_affinity_and_says_why(api):
    body = TestClient(api.app).get(f"/related-products/{LAPTOP}?score_by=learned").json()
    assert body["source"] == "co_occurrence" and body["ranked_by"] == "affinity"
    assert "no trained model" in body["learned_unavailable"]


def test_learned_ranks_with_the_model(api, tmp_path):
    write_model(tmp_path / "ranker")
    api._db["replay_runs"].insert_one(dict(RUN))
    client = TestClient(api.app)
    body = client.get(f"/related-products/{LAPTOP}?score_by=learned&limit=3").json()
    assert body["source"] == "learned" and body["model_version"] == "test"
    ids = [r["product_id"] for r in body["related_products"]]
    assert len(ids) == 3 and LAPTOP not in ids and len(set(ids)) == 3
    assert all(r["candidate_from"] for r in body["related_products"])
    metrics = client.get("/metrics").text
    assert 'affinity_related_products_total{source="learned"} 1' in metrics
    assert "affinity_ranker_seconds_count 1" in metrics


def test_learned_on_demo_traffic_falls_back(api, tmp_path):
    write_model(tmp_path / "ranker")                 # but no replay recorded
    body = TestClient(api.app).get(f"/related-products/{LAPTOP}?score_by=learned").json()
    assert body["source"] == "co_occurrence"
    assert "demo traffic" in body["learned_unavailable"]
