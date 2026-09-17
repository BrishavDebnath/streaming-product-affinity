# ADR 0009 - Parallel by default, and one command for the whole stack

**Status:** accepted

## Context
The topic had one partition, Spark ran with its default state store and
shuffle settings, checkpoints lived in a folder bind-mounted from the host,
and the API, dashboard, producer and seed ran in four host terminals.

## Problems
- One Kafka partition means one Spark read task, whatever the core count.
- The default (HDFS-backed) state store keeps every state row on the JVM
  heap; the co-occurrence join holds the most state, so heap is the ceiling.
- Checkpoints on a Windows bind mount are slow (thousands of small files
  crossing the VM boundary) and were easy to delete by accident or to leave
  stale after a reset.
- Four terminals, host networking and a Python install on the host were the
  main reasons a visitor could fail to run the project.

## Decision
- `kafka-init` creates `clickstream` with `KAFKA_PARTITIONS` (6) partitions;
  events are keyed by user, so a user's events stay in order.
- `SHUFFLE_PARTITIONS` (8) matches the `local[8]` master instead of Spark's
  default of 200, which would schedule 200 near-empty tasks per batch.
- `STATE_STORE=rocksdb` with changelog checkpointing: state lives off-heap and
  each checkpoint writes only the change. `hdfs` stays available.
- Checkpoints go to the `spark_checkpoints` named volume.
- Every process runs in Compose. Start-up order is expressed as dependencies
  (`service_healthy`, `service_completed_successfully`), not sleeps.

## Rejected
- **More partitions (24+).** On one machine with 8 Spark cores they add
  scheduling overhead without adding parallelism.
- **Keeping the host-terminal workflow as the default.** Kept only as an
  optional section in the README for editing code with live reload.

## Consequences
`docker compose up -d --build` is the whole quick start. Resetting is
`docker compose down -v`. Changing `KAFKA_PARTITIONS` on an existing topic has
no effect: `--if-not-exists` leaves it alone, so reset first. The throughput
figures these settings give are measured in Phase 3 (`docs/BENCHMARKS.md`).
