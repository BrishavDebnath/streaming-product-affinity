# Evaluation

Measured by `scripts/evaluate.py` on the RetailRocket dataset: real visits from
days the pipeline never saw, scored against the ten most-viewed products of the
training period. Method and caveats: [ADR 0011](adr/0011-real-data-and-evaluation.md).

<!-- hitrate:start -->
_2026-09-18T18:13:43+00:00, k=10, source=api, 3,000 test cases_

| Model | hit-rate@10 | Hits | Coverage |
|---|---:|---:|---:|
| **Pipeline** (co-occurrence, served by the API) | **17.43%** | 523 | 89% |
| Bestsellers (top 10 of the training days) | 0.73% | 22 | 100% |

Training: 30 days from 2015-05-03 (617,109 events), replayed through Kafka. Test: the following 7 day(s), 89,483 visits, never seen by the pipeline.

The pipeline is **23.77x** the baseline.
<!-- hitrate:end -->

## Reading those numbers

The table above is what a client actually receives from `/related-products`:
co-occurrence where the pipeline has pairs, trending products where it does
not. Running the same 3,000 cases against the pair table alone
(`--source mongo`) separates the two:

| Source | hit-rate@10 | Coverage | vs baseline |
|---|---:|---:|---:|
| MongoDB - co-occurrence only, ranked by pair count | **19.73%** | 64% | 26.91x |
| API - co-occurrence plus the trending fallback | 17.43% | 89% | 23.77x |
| Bestsellers - top 10 of the training month | 0.73% | 100% | - |

Read that as a precision/coverage trade rather than a ranking. The pair table
answered 1,932 of the 3,000 cases and was right about one in five of them; the
API answered 2,673, because when it has no pairs it falls back to trending, and
those extra answers are mostly wrong - which is the correct behaviour for a
"customers also viewed" strip that has to render something, and the wrong
behaviour for a number you want to quote. **19.7% at 64% coverage is what the
co-occurrence model itself is worth. 17.4% at 89% is what the product does.**

### Raw counts beat weighted affinity

The pair table is ranked by `pair_count` and the API ranks by `affinity`
(the count weighted by how unusual the pairing is). On this dataset the plain
count wins by 2.3 points. Weighting rewards pairs that are distinctive but
rare, and rare pairs are exactly the ones a 30-day window measures badly - a
pair seen four times has a wonderful affinity score and no evidence behind it.
`scripts/evaluate.py --score-by {affinity,lift,pmi}` exists so this can be
re-measured rather than argued about, and `MIN_PAIR_COUNT` is the knob that
decides how much evidence a pair needs before it is allowed to rank at all.

**What makes this conservative.** 1,068 of the 3,000 query products had no
pairs in the training month at all - real cold start - and every one counts as
a miss for the pipeline while the bestseller list still answers. Training on
more days, or lowering `MIN_PAIR_COUNT`, would raise coverage; these numbers
are 30 days of training with unchanged defaults.

**What it does not say.** This measures a "customers also viewed" strip, not
personalisation: the model sees one product, not a shopper's history. The
looser `any-product` figure - whether anything recommended appeared later in
the same visit, not just the immediate next view - is reported by the script
too, and is the easier question.

## How these numbers are kept honest

The pipeline's rows carry replay-clock timestamps, not dataset dates, so no
query on `product_pairs` can tell you which days of 2015 produced them. That
makes one mistake invisible and very expensive: splitting the dataset at day 7
while the pipeline holds a 30-day replay tests the model on days it was trained
on, and scores **35.1%** - double the truth - without erroring.

So `scripts/replay.py` writes what it sent to a `replay_runs` manifest, and
`scripts/evaluate.py` refuses to measure against a training window that is not
the window replayed. Reproducing the table above is therefore a fixed sequence with
no judgement in it:

```bash
docker compose down -v                              # nothing carried over
docker compose up -d --build
docker compose run --rm replay --days 30            # writes the manifest
docker compose run --rm evaluate --train-days 30 --test-days 7 --source mongo
docker compose run --rm evaluate --train-days 30 --test-days 7 --source api
```

Any other `--train-days` exits 2 and says why. The full account is
[defect 85](../DEFECTS_FIXED.md).
