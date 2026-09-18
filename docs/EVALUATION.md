# Evaluation

Measured by `scripts/evaluate.py` on the RetailRocket dataset: real visits from
days the pipeline never saw, scored against the ten most-viewed products of the
training period. Method and caveats: [ADR 0011](adr/0011-real-data-and-evaluation.md).

<!-- hitrate:start -->
_2026-09-18T04:46:41+00:00, k=10, source=api, 3,000 test cases_

| Model | hit-rate@10 | Hits | Coverage |
|---|---:|---:|---:|
| **Pipeline** (co-occurrence, served by the API) | **9.13%** | 274 | 73% |
| Bestsellers (top 10 of the training days) | 0.73% | 22 | 100% |

Training: 7 days from 2015-06-02 (144,671 events), replayed through Kafka. Test: the following 7 day(s), 58,951 visits, never seen by the pipeline.

The pipeline is **12.45x** the baseline.
<!-- hitrate:end -->

## Reading those numbers

The table above is what a client actually receives from `/related-products`:
co-occurrence where the pipeline has pairs, trending products where it does
not. Running the same 3,000 cases against the pair table alone
(`--source mongo`) separates the two:

| Source | hit-rate@10 | Coverage | vs baseline |
|---|---:|---:|---:|
| API — co-occurrence plus the trending fallback | 9.13% | 73% | 12.45x |
| MongoDB — co-occurrence only, no fallback | 7.90% | 33% | 10.77x |
| Bestsellers — top 10 of the training week | 0.73% | 100% | — |

The two rows differ in two ways, not one. The API answers from the recent
lookback where it can, widens to the whole retained history when that is
empty, and only then falls back to trending; the MongoDB row applies the
recent lookback strictly and nothing else. So the 1.2-point gap is part
history-widening and part trending fallback, and the strict co-occurrence
number - 7.9% at 33% coverage - is the conservative one to quote.

**What makes this conservative.** 796 of the 3,000 query products never
appeared in the training week at all - real cold start - and every one of them
counts as a miss for the pipeline while the bestseller list still answers.
Raising `MIN_PAIR_COUNT` or training on more days would raise coverage; the
numbers here are one week of training, unchanged defaults.

**What it does not say.** This measures a "customers also viewed" strip, not
personalisation: the model sees one product, not a shopper's history. The
looser `any-product` figure - whether anything recommended appeared later in
the same visit, 11.2% against 1.1% - is reported by the script too, and is the
easier question.
