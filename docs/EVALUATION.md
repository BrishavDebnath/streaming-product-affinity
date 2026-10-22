# Evaluation

Measured by `scripts/evaluate.py` on the RetailRocket dataset: real visits from
days the pipeline never saw, scored against the ten most-viewed products of the
training period. Method and caveats: [ADR 0011](adr/0011-real-data-and-evaluation.md).

<!-- hitrate:start -->
_2026-10-21T19:02:29+00:00, k=10, source=api, 3,000 test cases_

| Model | hit-rate@10 | Hits | Coverage |
|---|---:|---:|---:|
| **Pipeline, learned re-ranker** (LightGBM over co-occurrence, category, item2vec and bestseller candidates, served by the API) | **34.00%** | 1,020 | 100% |
| Pipeline, score_by=affinity (same cases) | 26.23% | 787 | n/a |
| Category bestsellers (top 10 of the query's category) | 20.77% | 623 | 100% |
| Bestsellers (top 10 of the training days) | 0.73% | 22 | 100% |

Training: 30 days from 2015-05-03 (617,109 events), replayed through Kafka. Test: the following 7 day(s), 89,483 visits, never seen by the pipeline.

Against the category bestsellers the pipeline is **+13.23 points**, and it is 46.36x the overall bestsellers.
<!-- hitrate:end -->

## Reading those numbers

The same 3,000 cases, scored five ways:

| Method | hit-rate@10 | Coverage | vs category bestsellers |
|---|---:|---:|---:|
| API, `score_by=learned`: the LightGBM re-ranker | **34.00%** | 100% | **+13.23 points** |
| API, `score_by=affinity`: co-occurrence, then the query's category, then trending | 26.23% | 100% | +5.46 points |
| MongoDB: co-occurrence only | 19.73% | 64% | minus 1.03 points |
| Category bestsellers | 20.77% | 100% | n/a |
| Global bestsellers | 0.73% | 100% | minus 20.03 points |

The co-occurrence-only and affinity rows come from the September run of the
same 3,000 cases (affinity then scored 26.17%; scored again next to the
re-ranker it was 26.23%, a two-hit difference from a fresh replay).

Category bestsellers is the baseline to beat. Co-occurrence alone does not
beat it: it answers 64% of questions and loses the rest. Affinity does,
because it fills the slots co-occurrence leaves empty with the query
category's most active products. The re-ranker beats affinity by **+7.77
points, 95% interval +6.43 to +9.00** (paired bootstrap over the same cases,
1,000 resamples, `--compare-to affinity`).

## The re-ranker

How it was trained, so that no step sees its own test days:

| Step | Window | Checked by |
|---|---|---|
| Features for training | replay of days 0 to 23 | `train_ranker.py` against the replay manifest |
| Labels (where shoppers went next) | days 23 to 30 | not in the replay |
| Features for serving, item2vec retrained | replay of days 0 to 30 | the API checks the vectors against the manifest |
| Test | days 30 to 37 | `evaluate.py` against the manifest |

The 34.0% sits close to the withdrawn 35.1% below, which is a coincidence
worth ruling out rather than ignoring. The 35.1% came from affinity scored
on days the pipeline held. The 34.0% came from the re-ranker on days 30 to
37, while the pipeline held days 0 to 30, and `evaluate.py` refused to run
until the manifest said so.

### Three separate test weeks

One week can be luck, so the same procedure ran on two more windows, each
with its own model trained on its own days (`scripts/live_folds.sh
2015-06-02 2015-07-02`). Every row is 3,000 held-out visits scored both ways
through the API, with a paired bootstrap over those visits.

| Test week | Learned | Affinity | Category bestsellers | Gain | 95% interval |
|---|---:|---:|---:|---:|---|
| days 30 to 37 (replay from 2015-05-03) | 34.00% | 26.23% | 20.77% | **+7.77** | +6.43 to +9.00 |
| days 60 to 67 (replay from 2015-06-02) | 30.50% | 23.10% | 18.60% | **+7.40** | +6.13 to +8.70 |
| days 90 to 97 (replay from 2015-07-02) | 30.00% | 21.30% | 18.53% | **+8.70** | +7.37 to +10.00 |

In every week the model answered every query product itself, with no
fallbacks. Candidate recall on the training labels was 47.0%, 45.9% and
36.9%; the third window has more visits and more items the earlier days
never showed.

Offline, the re-ranker gained 3.0 to 3.6 points ([ML_PLAN.md](ML_PLAN.md)).
Live it gains 7.4 to 8.7. The likeliest reason is the candidate pool:
offline the ranker drew co-occurrence candidates only from pairs seen at
least 3 times, while live it takes every pair and lets `passes_min_pairs`
weigh the thin ones. That is an explanation, not a measurement.

A first run of the two extra weeks reported "+0.00 points" for one of them.
The fold script had let the evaluation restart the API with another
window's model, which the API refused, so every learned answer was
affinity's. `evaluate.py` now exits with an error when the model answers
under half the queries, and prints the API's reason.

The features that carried the model, by gain: the share of the query's pairs
that go to the candidate, same parent category, lift, rank among the query's
co-occurrences, the query's popularity, same category, item2vec similarity
and cart rate.

Latency, measured over HTTP on 300 random catalogue items after the cache
was warm: **11.7 ms at the median and 34.1 ms at the 95th percentile**. The
ranker's own work (features plus LightGBM) averaged about 2 ms. The first
request after a restart takes about 5 s, because it builds the whole-window
aggregates, which are then cached for `RANKER_SNAPSHOT_SECONDS`.

Every query product had a category in the catalogue used here, built for all
235,061 items with `scripts/replay.py --dry-run --days 140`.

**What it does not measure.** This is a "customers also viewed" strip, not
personalisation: the model sees one product, not a shopper's history.

## How these numbers are kept honest

The pipeline's rows carry replay-clock timestamps, not dataset dates, so no
query on `product_pairs` can tell you which days of 2015 produced them. That
makes one mistake both invisible and expensive. If you split the dataset at
day 7 while the pipeline holds a 30-day replay, you test the model on days it
was trained on. It scores **35.1%**, double the true figure, and nothing
errors.

So `scripts/replay.py` records what it sent in a `replay_runs` manifest, and
`scripts/evaluate.py` refuses to measure against any training window other
than the one replayed. Reproducing the table above is a fixed sequence with no
judgement calls:

```bash
docker compose down -v                              # nothing carried over
docker compose up -d --build
docker compose run --rm replay --days 30            # writes the manifest
docker compose run --rm evaluate --train-days 30 --test-days 7 --source mongo
docker compose run --rm evaluate --train-days 30 --test-days 7 --source api
```

The re-ranker adds a training replay before this and an item2vec retrain
after the serving replay; the README lists the full sequence.

Any other `--train-days` exits 2 and says why. The full account is
[defect 85](../DEFECTS_FIXED.md).
