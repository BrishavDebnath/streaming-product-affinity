# Shortcuts for macOS / Linux. On Windows, run the docker compose commands
# shown in README.md directly - they do the same thing.
.PHONY: help up down clean ps logs test test-all lint smoke loadtest recovery seed \
        dataset replay evaluate

help:
	@echo "make up         - build and start the whole stack"
	@echo "make ps         - show container status"
	@echo "make logs       - follow the Spark job's log"
	@echo "make test       - run the Spark tests inside the Spark image"
	@echo "make test-all   - run every test here (needs requirements-test.txt)"
	@echo "make lint       - ruff and mypy"
	@echo "make smoke      - end-to-end check against the running stack"
	@echo "make loadtest   - throughput and latency benchmark (~25 min)"
	@echo "make recovery   - kill and restart Spark, check nothing was lost"
	@echo "make seed       - backfill 20 more minutes of history"
	@echo "make dataset    - download the RetailRocket dataset (needs a Kaggle token)"
	@echo "make replay     - replay a week of real traffic through Kafka (~5 min)"
	@echo "make evaluate   - hit-rate@10 against the bestseller baseline"
	@echo "make down       - stop everything, keep the data"
	@echo "make clean      - stop everything and delete all data"

up:
	docker compose up -d --build
	@echo ""
	@echo "Dashboard   http://localhost:8501"
	@echo "API docs    http://localhost:8000/docs"
	@echo "Prometheus  http://localhost:9090"
	@echo "Grafana     http://localhost:3000  (dashboard: Streaming Product Affinity Pipeline)"

ps:
	docker compose ps

logs:
	docker compose logs -f spark

# --no-deps: Kafka and Mongo are not started. --rm: nothing is left behind.
test:
	docker compose run --rm --no-deps spark \
		/opt/spark/bin/spark-submit /app/tests/test_transforms.py

# Everything, including the API and dashboard tests, on your own machine.
test-all:
	pytest --cov --cov-report=term-missing

lint:
	ruff check .
	mypy

smoke:
	docker compose run --rm smoke

loadtest:
	docker compose run --rm loadtest

recovery:
	docker compose run --rm recovery

seed:
	docker compose run --rm --no-deps seed python scripts/seed.py --minutes 20 --sessions 400

# Real data (docs/adr/0011). The download runs on the host, because it needs
# your Kaggle token; the replay and the evaluation run in the stack.
dataset:
	pip install -r requirements-data.txt
	python scripts/fetch_dataset.py

replay:
	docker compose run --rm replay --days 30

evaluate:
	docker compose run --rm evaluate --train-days 30 --test-days 7

down:
	docker compose down

clean:
	docker compose down -v
