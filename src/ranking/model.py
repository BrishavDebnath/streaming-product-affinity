"""
Loading and using a trained re-ranker.

A model directory, as scripts/train_ranker.py writes it:

    model.txt              the LightGBM booster
    metadata.json          feature names in order, training window, metrics
    category_parents.json  category -> parent category, from the dataset
    item2vec.npz           item vectors for the window the pipeline serves
    item2vec.json          which replay window those vectors were trained on

Two checks refuse a model instead of letting it answer quietly wrong:

1. Its feature list must equal `features.FEATURE_NAMES`, in order. A model
   trained on other columns would read every feature in the wrong slot.
2. The vectors must come from the window the pipeline currently holds (the
   replay manifest). Vectors from another window describe other items.

Either failure leaves the API answering with plain co-occurrence, and the
response says why.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from src.ranking import features

log = logging.getLogger("ranking")


class ModelError(Exception):
    """The model directory cannot be used as it stands."""


class Ranker:
    def __init__(self, booster, metadata: dict, parent: dict[str, str | None],
                 embeddings: features.Embeddings | None, embeddings_window: dict | None):
        self.booster = booster
        self.metadata = metadata
        self.parent = parent
        self.embeddings = embeddings
        self.embeddings_window = embeddings_window

    @property
    def version(self) -> str:
        return str(self.metadata.get("version", "unknown"))

    @property
    def trained_at(self) -> datetime | None:
        """When the trainer saved it: the version is that UTC timestamp."""
        try:
            return datetime.strptime(self.version, "%Y%m%dT%H%M%SZ").replace(
                tzinfo=timezone.utc)
        except ValueError:
            return None

    def score(self, matrix: np.ndarray) -> np.ndarray:
        if len(matrix) == 0:
            return np.zeros(0)
        # One thread: a query has about 60 rows, and starting LightGBM's thread
        # pool for them cost 11 ms against 0.5 ms single-threaded (measured).
        return np.asarray(self.booster.predict(matrix, num_threads=1), dtype=float)

    def check_window(self, replay_run: dict | None) -> str | None:
        """Why the vectors do not fit what the pipeline holds, or None."""
        if self.embeddings is None:
            return "the model directory has no item2vec vectors"
        if not replay_run:
            return ("no replay is recorded, so the pipeline is serving demo traffic "
                    "the model was never trained on")
        want = self.embeddings_window or {}
        if (want.get("start_day") != replay_run.get("start_day")
                or float(want.get("days", -1)) != float(replay_run.get("days", -2))):
            return (f"the vectors were trained on {want.get('days')} days from "
                    f"{want.get('start_day')}, but the pipeline holds "
                    f"{replay_run.get('days')} days from {replay_run.get('start_day')}. "
                    f"Run scripts/train_ranker.py --embeddings-only for this window")
        return None


def feature_difference(model: tuple, code: tuple) -> str:
    """Say which features differ, not all of them: the first version of this
    message printed both full lists and left the reader to compare them."""
    only_model = [n for n in model if n not in code]
    only_code = [n for n in code if n not in model]
    parts = []
    if only_model:
        parts.append(f"only in the model: {', '.join(only_model)}")
    if only_code:
        parts.append(f"only in the code: {', '.join(only_code)}")
    return "; ".join(parts) or "the same names in a different order"


def load(directory: str | Path) -> Ranker:
    """Load a model directory, or raise ModelError saying what is wrong."""
    path = Path(directory)
    meta_file, model_file = path / "metadata.json", path / "model.txt"
    if not meta_file.is_file() or not model_file.is_file():
        raise ModelError(f"no trained model in {path}")
    metadata = json.loads(meta_file.read_text(encoding="utf-8"))
    names = tuple(metadata.get("feature_names", ()))
    if names != features.FEATURE_NAMES:
        raise ModelError(f"the model's features differ from this code's "
                         f"({feature_difference(names, features.FEATURE_NAMES)}); "
                         f"retrain it")

    import lightgbm as lgb  # only needed once a model exists

    booster = lgb.Booster(model_file=str(model_file))
    parents_file = path / "category_parents.json"
    parent = (json.loads(parents_file.read_text(encoding="utf-8"))
              if parents_file.is_file() else {})
    vectors, window_file = path / "item2vec.npz", path / "item2vec.json"
    embeddings = features.Embeddings.load(vectors) if vectors.is_file() else None
    window = (json.loads(window_file.read_text(encoding="utf-8"))
              if window_file.is_file() else None)
    log.info("loaded ranker %s from %s", metadata.get("version"), path)
    return Ranker(booster, metadata, parent, embeddings, window)
