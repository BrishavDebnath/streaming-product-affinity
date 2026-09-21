# Image for the Python services: API, dashboard, producer, seed, smoke test.
# The Spark job has its own image (Dockerfile.spark).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

WORKDIR /app

# Graphviz draws the affinity graph on the server, so the dashboard can show a
# pair's count in a hover box instead of printing thirty overlapping numbers.
RUN apt-get update \
 && apt-get install -y --no-install-recommends graphviz fonts-dejavu-core \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src ./src
COPY scripts ./scripts

# Run as an unprivileged user; Streamlit needs a writable home directory.
RUN useradd --create-home --uid 10001 app
USER app
