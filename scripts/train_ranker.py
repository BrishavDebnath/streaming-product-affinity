#!/usr/bin/env python3
"""
Train the learned re-ranker against the live pipeline.

    docker compose run --rm replay --days 23          # the feature window
    python scripts/train_ranker.py --train-days 23    # labels: the 7 days after

    docker compose down -v && docker compose up -d --build
    docker compose run --rm replay --days 30          # the window to serve
    python scripts/train_ranker.py --train-days 30 --embeddings-only

Why two replays: the model learns from features the pipeline computed over
days 0-23, labelled with where shoppers went next in days 23-30. It then
serves from a 30-day replay and is tested on days 30-37, which neither step
has seen. The item2vec vectors describe the window being served, so they are
retrained for it (`--embeddings-only`) while the ranker stays as trained.

Features come from `src/ranking/features.py` reading MongoDB, the same code
path the API uses, so training and serving cannot drift apart.

Needs `pip install -r requirements-ml.txt` and the stack's MongoDB on
localhost:27018 (the default MONGO_URI).
"""

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np  # noqa: E402

from src.common import config, mongo  # noqa: E402
from src.data import evaluation as ev  # noqa: E402
from src.data import retailrocket as rr  # noqa: E402
from src.ranking import features  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
OUT = ROOT / "models" / "ranker"
SEED = 7

# The feasibility run's settings (experiments/ml-feasibility/common.py).
RANKER_PARAMS: dict[str, Any] = {
    "objective": "lambdarank", "n_estimators": 300, "learning_rate": 0.05,
    "num_leaves": 31, "min_child_samples": 50, "subsample": 0.8,
    "subsample_freq": 1, "colsample_bytree": 0.8, "random_state": SEED, "verbose": -1,
}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s train | %(message)s")
log = logging.getLogger("train")


def day_of(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%d")


def read_windows(path: Path, start: str | None, train_days: float, label_days: float):
    """Visits in the training window and in the label window after it."""
    first, _ = rr.time_span(str(path))
    begin = (datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
             if start else first)
    train, label = [], []
    for event in rr.read_events(str(path)):
        offset = (event.at - begin) / 86400
        if 0 <= offset < train_days:
            train.append(event)
        elif train_days <= offset < train_days + label_days:
            label.append(event)
    return (begin, list(rr.sessionise(rr.sort_events(train))),
            list(rr.sessionise(rr.sort_events(label))))


def load_parents(path: Path) -> dict[str, str | None]:
    parent: dict[str, str | None] = {}
    for line in path.read_text(encoding="utf-8").splitlines()[1:]:
        child, par = line.strip().split(",")
        parent[f"cat-{child}"] = f"cat-{par}" if par else None
    return parent


def load_categories(path: Path) -> dict[int, str]:
    return {p["id"]: p["category"] for p in json.loads(path.read_text(encoding="utf-8"))
            if p.get("category") not in (None, "uncategorised")}


def train_item2vec(visits) -> features.Embeddings:
    """Skip-gram over visits, as in the feasibility run. One worker thread,
    so the same window always gives the same vectors."""
    from gensim.models import Word2Vec

    sentences = [[str(i) for i in v.items] for v in visits if len(v.items) >= 2]
    model = Word2Vec(sentences, vector_size=64, window=5, min_count=2, sg=1,
                     negative=10, epochs=15, workers=1, seed=SEED, sample=1e-4)
    ids = np.array([int(k) for k in model.wv.index_to_key], dtype=np.int64)
    return features.Embeddings(ids, model.wv.vectors.copy())


def save_embeddings(emb: features.Embeddings, out: Path, window: dict) -> None:
    out.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(out / "item2vec.npz", ids=emb.ids, vectors=emb.unit)
    (out / "item2vec.json").write_text(json.dumps(window, indent=2), encoding="utf-8")
    log.info("wrote %s item vectors for %g days from %s", f"{len(emb.ids):,}",
             window["days"], window["start_day"])


def git_commit() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT,
                              capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--events", type=Path, default=RAW / "events.csv")
    parser.add_argument("--catalog", type=Path,
                        default=ROOT / "data" / "catalog_retailrocket.json")
    parser.add_argument("--tree", type=Path, default=RAW / "category_tree.csv")
    parser.add_argument("--train-days", type=float, required=True,
                        help="must match the replay the pipeline holds")
    parser.add_argument("--label-days", type=float, default=7.0)
    parser.add_argument("--start", type=str, default=None,
                        help="first replayed day, YYYY-MM-DD (must match the replay)")
    parser.add_argument("--out", type=Path, default=OUT)
    parser.add_argument("--embeddings-only", action="store_true",
                        help="retrain only the item vectors, for the window now replayed")
    args = parser.parse_args()

    for needed in (args.events, args.catalog, args.tree):
        if not needed.is_file():
            log.error("%s not found - run scripts/fetch_dataset.py and "
                      "scripts/replay.py --dry-run --days 140 first", needed)
            return 2

    started = time.time()
    begin, train_visits, label_visits = read_windows(
        args.events, args.start, args.train_days,
        0 if args.embeddings_only else args.label_days)
    window = {"start_day": day_of(begin), "days": args.train_days}

    db = mongo.client(config.MONGO_URI)[config.MONGO_DB]
    run = db["replay_runs"].find_one(sort=[("finished_at", -1)])
    if not run or run.get("start_day") != window["start_day"] \
            or float(run.get("days", 0)) != args.train_days:
        log.error("the pipeline holds %s, but this asks for %g days from %s. "
                  "Replay that window first.",
                  f"{run.get('days')} days from {run.get('start_day')}" if run
                  else "no recorded replay", args.train_days, window["start_day"])
        return 2

    log.info("item2vec on %s training visits", f"{len(train_visits):,}")
    emb = train_item2vec(train_visits)
    if args.embeddings_only:
        if not (args.out / "model.txt").is_file():
            log.error("no model in %s to attach the vectors to; train one first", args.out)
            return 2
        save_embeddings(emb, args.out, window)
        return 0

    import lightgbm as lgb

    categories = load_categories(args.catalog)
    parent = load_parents(args.tree)
    cutoff = features.anchored_cutoff(db)
    snap = features.Snapshot.from_mongo(db, cutoff, category_of=categories.get,
                                        parent=parent, embeddings=emb)
    cases = ev.test_cases(label_visits)
    log.info("building features for %s label cases", f"{len(cases):,}")

    X, y, groups = [], [], []
    found = 0
    neighbours: dict[int, dict[int, float]] = {}
    for case in cases:
        if case.query not in neighbours:
            neighbours[case.query] = features.neighbours_from_mongo(db, case.query, cutoff)
        cands, matrix, _ = features.rows(case.query, neighbours[case.query], snap)
        if case.target not in cands:
            continue            # no positive to learn from
        found += 1
        X.append(matrix)
        y += [int(c == case.target) for c in cands]
        groups.append(len(cands))
    if not groups:
        log.error("no label case had its target among the candidates")
        return 1
    recall = found / len(cases)
    log.info("candidate recall %.1f%%: %s of %s cases have the right item among "
             "the candidates", 100 * recall, f"{found:,}", f"{len(cases):,}")

    model = lgb.LGBMRanker(**RANKER_PARAMS)
    model.fit(np.vstack(X), np.array(y), group=groups)

    args.out.mkdir(parents=True, exist_ok=True)
    model.booster_.save_model(str(args.out / "model.txt"))
    gains = model.booster_.feature_importance("gain")
    metadata = {
        "version": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "feature_names": list(features.FEATURE_NAMES),
        "feature_window": window,
        "label_days": args.label_days,
        "label_cases": len(cases),
        "training_groups": len(groups),
        "training_rows": int(sum(groups)),
        "candidate_recall": round(recall, 4),
        "params": RANKER_PARAMS,
        "feature_gain": {n: round(float(g), 1) for n, g in
                         sorted(zip(features.FEATURE_NAMES, gains, strict=True),
                                key=lambda kv: -kv[1])},
        "lightgbm": lgb.__version__,
        "git_commit": git_commit(),
        "seconds": round(time.time() - started, 1),
    }
    (args.out / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    (args.out / "category_parents.json").write_text(json.dumps(parent), encoding="utf-8")
    save_embeddings(emb, args.out, window)
    log.info("trained on %s cases (%s rows) in %.0fs; wrote %s", f"{len(groups):,}",
             f"{sum(groups):,}", time.time() - started, args.out)
    log.info("most useful features: %s", ", ".join(list(metadata["feature_gain"])[:6]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
