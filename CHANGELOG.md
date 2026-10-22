# Changelog

All notable changes. Numbers are hit-rate@10 on 3,000 held-out RetailRocket
visits through the live API unless a line says otherwise; the method is in
[docs/EVALUATION.md](docs/EVALUATION.md).

## 1.2.0 · 2026-10-22

### Added
- **Two-hop and parent-category candidates for the re-ranker.** Two-hop
  proposes products two steps away (the query goes with B, B goes with C),
  scored by the summed path counts through the query's ten strongest
  neighbours, in one MongoDB query. Parent-category candidates are the most
  viewed items one level up the category tree. Three new features
  (`log_two_hop`, `two_hop_rank`, `parent_rank`) bring the model to 19.
- **Measured against the previous model on the same data**, three test
  weeks, each with its own model:

  | Test week | Candidate recall | v1.2 | v1.1 | v1.2 vs v1.1 (95% interval) |
  |---|---|---:|---:|---|
  | days 30–37 | 47.0% → 49.3% | 35.70% | 34.37% | +1.33 (+0.47 to +2.17) |
  | days 60–67 | 45.9% → 49.0% | 31.07% | 30.50% | +0.57 (−0.33 to +1.53) |
  | days 90–97 | 36.9% → 38.8% | 30.20% | 30.10% | +0.10 (−0.67 to +0.90) |

  A small gain: about +0.7 points pooled, clearly above zero in one week of
  three, never a loss. Latency 12.9 ms at the median (was 11.7).
- `evaluate.py --compare-url`: score another API, such as the previous build
  serving the previous model, on the same pipeline data, with a paired
  bootstrap. `scripts/live_folds.sh` takes `MODEL_PREFIX`, `BASELINE_IMAGE`
  and `BASELINE_MODELS` to run the previous model beside the new one.
- `scripts/screenshots.py`: captures the README's screenshots and demo GIF
  from the running dashboard in a headless browser. All screenshots retaken.

### Fixed
- Choosing "Learned model" on the dashboard sometimes showed "No data for
  this product yet". The first answer after the ranker's cache expires takes
  about 5 s, and the dashboard gave up at 5 s. It now waits 30 s for that
  call. Found while scripting the screenshots.
- Text that is not JSON, and `{}`, went to the dead-letter queue as
  `missing_or_invalid:user_id`. They now say `not_json_or_empty`. Found by
  sending bad JSON into Kafka on purpose.
- The `RankerUnusable` alert fired for as long as the demo ran, because demo
  traffic never fits a model trained on real data. It now also needs a
  recorded replay (`affinity_replay_recorded`). Found by stopping the
  producer and reading the alert page.
- A model with a different feature list was refused with both full lists
  printed; the message now names only the features that differ.

## 1.1.0 · 2026-10-22

### Added
- **Learned re-ranker** (`/related-products?score_by=learned`). Candidates
  from co-occurrence (no minimum count), the query's category, item2vec
  neighbours and global bestsellers; 16 features from the aggregates Spark
  already writes; LightGBM LambdaRank. One feature function
  (`src/ranking/features.py`) is shared by the trainer and the API, so
  training and serving cannot drift. The API refuses a model with another
  feature list or item2vec vectors from another replay window, and answers
  as affinity, saying why.
- `scripts/train_ranker.py`: features from a replayed window, labels from
  the week after, guarded by the replay manifest.
- Trending rows carry view, add-to-cart and purchase counts.
- **Results on three separate test weeks**, each with its own model:

  | Test week | Learned | Affinity | Category bestsellers | Gain (95% interval) |
  |---|---:|---:|---:|---|
  | days 30–37 | 34.00% | 26.23% | 20.77% | +7.77 (+6.43 to +9.00) |
  | days 60–67 | 30.50% | 23.10% | 18.60% | +7.40 (+6.13 to +8.70) |
  | days 90–97 | 30.00% | 21.30% | 18.53% | +8.70 (+7.37 to +10.00) |

- `evaluate.py --compare-to`: two rankings on the same cases with a paired
  bootstrap 95% interval. `scripts/live_folds.sh` runs train, serve and score
  for any test week.
- Monitoring for the model: `affinity_ranker_loaded`, `_usable`,
  `_model_age_seconds`, latency and fallbacks on `/metrics`; a Grafana row;
  alerts `RankerModelStale`, `RankerUnusable` and `RankerFallingBack`.
- Dashboard: rank related products by times seen together, lift or the
  learned model, with the candidate source on every row.
- `MONGO_IMAGE` setting for hosts whose kernel MongoDB 8 cannot start on.

### Changed
- Re-ranker latency: 11.7 ms at the median and 34.1 ms at p95 over HTTP.
  LightGBM scores single-threaded (11 ms to 0.5 ms for ~60 rows), and the
  newest window is found through an index.
- `score_by=affinity`: per-product counts are cached per minute of cutoff
  with a 30 s TTL. p50 25 ms to 12.5 ms, p95 921 ms to 221 ms.
- Upgrades: pandas 3 (3.1.0 in the app image), Streamlit 1.66, uvicorn
  0.54, kafka-python 3.0.11, pytest 9, pytest-cov 7, ruff 0.17, mypy 2.4,
  actions/setup-java 6, CodeQL 4.39.
- Documentation trimmed: README to under 200 lines, `DEFECTS_FIXED.md` to the
  ten bugs worth knowing, five minor ADRs, `SECURITY.md` and
  `CONTRIBUTING.md` removed.

### Fixed
- CI's whole-stack job failed on some runs: MongoDB 8 refuses to start on
  Linux kernel 6.19+ (SERVER-121912), which hosted runners sometimes boot.
  CI now picks `mongo:7.0` on such kernels.
- `evaluate.py` refuses a `--score-by learned` run that the model did not
  answer, instead of reporting "+0.00 points" for two identical rankings.
- A test read the machine's live MongoDB and failed whenever a stack
  holding a real replay was running.

## 1.0.0 · 2026-09-21

First public release: Kafka 4, Spark 4 Structured Streaming, MongoDB, FastAPI
and a Streamlit dashboard, started with one `docker compose up`. Measured on
real RetailRocket traffic with a replay manifest guarding against train/test
leakage; 15,000 events/s sustained on a laptop; exactly-once results across a
crash; CI with lint, types, CodeQL and Dependabot.
