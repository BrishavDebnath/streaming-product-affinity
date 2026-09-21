# Contributing

This is a personal portfolio project rather than a library with users, so it
does not need a governance document. What it does need is for anyone reading
it - including me in six months - to be able to change it without breaking the
properties it claims. That is what this file is for.

## Getting it running

```bash
docker compose up -d --build          # the whole stack
docker compose run --rm smoke         # end-to-end check against it
```

For work on the code itself:

```bash
python -m venv .venv
.venv/bin/activate                    # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements-test.txt  # includes requirements.txt
pytest                                # 127 tests, no Kafka or MongoDB needed
ruff check . && mypy                  # the same checks CI runs
```

The Spark tests need Java 17+. If you would rather not install it, they also
run inside the image:

```bash
docker compose run --rm --no-deps spark \
  /opt/spark/bin/spark-submit /app/tests/test_transforms.py
```

## What a change needs

1. **A test that fails without it.** Every defect in `DEFECTS_FIXED.md` has
   one; that file is the project's memory of what has already gone wrong.
2. **`ruff check .` and `mypy` clean.** CI fails on either.
3. **The claim and the code agreeing.** The README states measured numbers and
   behaviour; if a change moves a number, the number moves in the README, and
   if it changes what an endpoint does, the endpoint's row changes too. A
   README that drifts from the code is worse than no README.
4. **An ADR for a decision, not for a detail.** `docs/adr/` records choices
   that are not obvious from the code, each with the alternative that was
   rejected. Add one when the *why* would otherwise be lost.

## Things that are easy to get wrong here

- **Changing a window size, the state store, or the partition count** needs a
  fresh start (`docker compose down -v`): Spark will not resume from a
  checkpoint whose plan has changed.
- **Anything stateful in Spark must be windowed.** A `groupBy` on business
  keys alone retains state for every key ever seen; the watermark cannot evict
  it. `tests/test_transforms.py` asserts state stays bounded, in a real
  streaming query.
- **Sinks must stay idempotent.** Writes upsert on a natural key so a re-run
  after a crash converges. A sink that inserts breaks the exactly-once result
  the recovery test checks.
- **Numbers on screen must say what window they came from.** Two endpoints
  once answered from the whole retained history while reporting a 30-minute
  lookback; both now label it. Keep that honest.

## Commits

Present tense, saying what changed and why the old way was wrong -
`git log` is the other half of `DEFECTS_FIXED.md`. Keep unrelated changes in
separate commits.
