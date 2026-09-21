# Evaluation

Measured by `scripts/evaluate.py` on the RetailRocket dataset: real visits from
days the pipeline never saw, scored against the ten most-viewed products of the
training period. Method and caveats: [ADR 0011](adr/0011-real-data-and-evaluation.md).

<!-- hitrate:start -->
_2026-09-21T16:06:29+00:00, k=10, source=api, 3,000 test cases_

| Model | hit-rate@10 | Hits | Coverage |
|---|---:|---:|---:|
| **Pipeline** (co-occurrence, then category fill, served by the API) | **26.17%** | 785 | 100% |
| Category bestsellers (top 10 of the query's category) | 20.77% | 623 | 100% |
| Bestsellers (top 10 of the training days) | 0.73% | 22 | 100% |

Training: 30 days from 2015-05-03 (617,109 events), replayed through Kafka. Test: the following 7 day(s), 89,483 visits, never seen by the pipeline.

Against the category bestsellers the pipeline is **+5.40 points**, and it is 35.68x the overall bestsellers.
<!-- hitrate:end -->

## Reading those numbers

The same 3,000 cases, scored four ways:

| Method | hit-rate@10 | Coverage | vs category bestsellers |
|---|---:|---:|---:|
| API: co-occurrence, then the query's category, then trending | **26.17%** | 100% | **+5.40 points** |
| MongoDB: co-occurrence only | 19.73% | 64% | minus 1.03 points |
| Category bestsellers | 20.77% | 100% | n/a |
| Global bestsellers | 0.73% | 100% | minus 20.03 points |

Category bestsellers is the baseline to beat. Co-occurrence alone does not
beat it: it answers 64% of questions and loses the rest. The API does, because
it fills the slots co-occurrence leaves empty with the query category's most
active products. Of the 3,000 answers, 753 came from pairs alone, 812 from
pairs topped up from the category, 971 from the category because the product
had no pairs, and 69 from trending.

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

Any other `--train-days` exits 2 and says why. The full account is
[defect 85](../DEFECTS_FIXED.md).
