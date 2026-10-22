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
# Each fold takes about 30 minutes. Results go to results/<model>.json and
# leave docs/EVALUATION.md alone. Needs the dataset, the full catalogue and
# requirements-ml.txt on the host (see README, "Is it any good?").
#
# To compare a new model with the previous one on the same pipeline data,
# name an image of the previous API build and the model each fold should use:
#
#     MODEL_PREFIX=twohop BASELINE_IMAGE=streaming-product-affinity-api:old #     BASELINE_MODELS="2015-05-03=ranker 2015-06-02=fold-2015-06-02" #         bash scripts/live_folds.sh 2015-05-03 2015-06-02
#
# Each fold then also starts that image beside the API, serving its model,
# and writes results/<model>-vs-baseline.json.
set -euo pipefail

cd "$(dirname "$0")/.."
export CATALOG_FILE=data/catalog_retailrocket.json
PY=${PYTHON:-python}
PREFIX=${MODEL_PREFIX:-fold}
# Docker on Windows wants a Windows path for a bind mount; Git Bash has both.
HOST_DIR=$(pwd -W 2>/dev/null || pwd)

baseline_model() {                 # the BASELINE_MODELS entry for one start date
    for pair in ${BASELINE_MODELS:-}; do
        [ "${pair%%=*}" = "$1" ] && { echo "${pair#*=}"; return 0; }
    done
    return 1
}

start_baseline() {
    docker rm -f affinity-api-baseline >/dev/null 2>&1 || true
    MSYS_NO_PATHCONV=1 docker run -d --name affinity-api-baseline         --network streaming-product-affinity_default         -e MONGO_URI=mongodb://mongo:27017 -e CATALOG_FILE="$CATALOG_FILE"         -e RANKER_DIR="/app/models/$1"         -v "$HOST_DIR/data:/app/data:ro" -v "$HOST_DIR/models:/app/models:ro"         "$BASELINE_IMAGE" uvicorn src.api.main:app --host 0.0.0.0 --port 8000 >/dev/null
    for _ in $(seq 1 60); do
        docker exec affinity-api-baseline python -c             "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"             >/dev/null 2>&1 && return 0
        sleep 3
    done
    echo "the baseline API never came up" >&2
    return 1
}

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
    model="$PREFIX-$start"
    # Exported for every compose call in the fold, not only the API's own: a
    # `compose run` of a service that depends on the API recreates the API
    # whenever its configuration differs, which put the default model back
    # and made every learned answer fall back.
    export RANKER_MODEL="$model"
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
    docker compose up -d --force-recreate --no-deps api
    for _ in $(seq 1 60); do curl -fs 127.0.0.1:8000/health >/dev/null && break; sleep 3; done

    echo "=== fold $start: evaluation"
    docker compose run --rm evaluate --start "$start" --train-days 30 --test-days 7 \
        --score-by learned --compare-to affinity --no-report
    cp results/evaluation.json "results/$model.json"

    if [ -n "${BASELINE_IMAGE:-}" ] && old=$(baseline_model "$start"); then
        echo "=== fold $start: against the previous model ($old)"
        start_baseline "$old"
        docker compose run --rm evaluate --start "$start" --train-days 30 --test-days 7             --score-by learned --compare-to learned             --compare-url http://affinity-api-baseline:8000 --no-report
        cp results/evaluation.json "results/$model-vs-baseline.json"
        docker rm -f affinity-api-baseline >/dev/null
    fi
done
