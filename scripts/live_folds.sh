#!/usr/bin/env bash
# The re-ranker against affinity on separate test weeks, through the live
# pipeline and API. One week can be luck; several that agree are not.
#
#     bash scripts/live_folds.sh 2015-06-02 2015-07-02
#
# For each start date S, with its own model in models/fold-S:
#   1. replay days S to S+23 and train on them, labels from S+23 to S+30
#   2. on a clean stack, replay S to S+30 and retrain item2vec for it
#   3. score learned and affinity on the same 3,000 visits from S+30 to S+37
#
# Each fold takes about 30 minutes. Results go to results/fold-S.json and
# leave docs/EVALUATION.md alone. Needs the dataset, the full catalogue and
# requirements-ml.txt on the host (see README, "Is it any good?").
set -euo pipefail

cd "$(dirname "$0")/.."
export CATALOG_FILE=data/catalog_retailrocket.json
PY=${PYTHON:-python}

fresh_stack() {
    docker compose down -v
    docker compose up -d spark api
    for _ in $(seq 1 60); do
        curl -fs 127.0.0.1:8000/health >/dev/null && return 0
        sleep 3
    done
    echo "the API never came up" >&2
    return 1
}

for start in "$@"; do
    model="fold-$start"
    echo "=== fold $start: training window"
    fresh_stack
    docker compose run --rm replay --start "$start" --days 23
    sleep 60                                   # the last pair windows close
    "$PY" scripts/train_ranker.py --start "$start" --train-days 23 --out "models/$model"

    echo "=== fold $start: serving window"
    fresh_stack
    docker compose run --rm replay --start "$start" --days 30
    sleep 60
    "$PY" scripts/train_ranker.py --start "$start" --train-days 30 --embeddings-only \
        --out "models/$model"
    RANKER_MODEL="$model" docker compose up -d --force-recreate --no-deps api
    for _ in $(seq 1 60); do curl -fs 127.0.0.1:8000/health >/dev/null && break; sleep 3; done

    echo "=== fold $start: evaluation"
    docker compose run --rm evaluate --start "$start" --train-days 30 --test-days 7 \
        --score-by learned --compare-to affinity --no-report
    cp results/evaluation.json "results/$model.json"
done
