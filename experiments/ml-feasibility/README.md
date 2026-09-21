# ML feasibility experiments

The measurements behind [docs/ML_PLAN.md](../../docs/ML_PLAN.md). They read the
RetailRocket export directly and approximate the pipeline offline, so they are
for deciding what to build, not for quoting. Published numbers come from the
live API through `scripts/evaluate.py`.

```bash
pip install -r requirements-ml.txt      # gensim, lightgbm, scikit-learn
python scripts/fetch_dataset.py         # data/raw/events.csv and friends
python scripts/replay.py --dry-run --days 140 # a catalogue covering every item, sends nothing
cd experiments/ml-feasibility
```

| Script | What it answers | Time on 2 cores |
|---|---|---:|
| `profile_data.py` | How long are visits, how many visitors return, how heavy are the heaviest, how often do visits convert | 1 min |
| `baselines.py` | Every simple method and the obvious ML ideas on the published split | 1 min |
| `ranker.py` | The learned re-ranker against the best non-ML method, and whether purchase intent is predictable | 4 min |
| `candidates.py` | How much each extra candidate source raises the ranker's ceiling | 2 min |
| `folds.py` | Whether the re-ranker's gain holds in three separate weeks, with bootstrap intervals | 15 to 20 min |

`common.py` holds the shared pieces: loading, the split, the pipeline's
co-occurrence rule (2-minute gap, minimum 3 pairs), and the feature set.

The split is always the same shape: features from the earlier days only,
labels from the week after, tests from the week after that. Nothing in a test
week is seen while training. Item2vec trains on two threads, so repeated runs
can differ by about a hundredth of a point.

The catalogue matters. `scripts/replay.py` writes one for the items it
replays, so after a 30-day replay it covers only that month. The first
measurements were taken with exactly that catalogue: 74% of items in the first
test period had a category, and 47 to 49% in the later two. Build it with
`--days 140` as above so every item has one.
