# Image for the Python services: API, dashboard, producer, seed, smoke test.
# The Spark job has its own image (Dockerfile.spark).
FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src ./src
COPY scripts ./scripts

# Run as an unprivileged user; Streamlit needs a writable home directory.
RUN useradd --create-home --uid 10001 app
USER app
