# ADR 0010 - How throughput, latency and recovery are measured

**Status:** accepted

## Context
The first load test sent two-event sessions of fixed products from one
process, polled `/pipeline` for "lag" (which is really the delay between a
window closing and its row being written) and asked the reader to fill in
the hardware by hand. Fault tolerance was a manual runbook step. Neither
produced a number that could be defended in an interview.

## Decisions
- **Realistic traffic, several processes.** The load generator uses the live
  producer's session generator, spread over separate processes, so the
  generator itself is less likely to be the bottleneck. It reports the rate
  it actually achieved next to the target.
- **Spark's own figures, per batch.** The job already records every batch in
  `pipeline_metrics`. The benchmark reads those instead of sampling the API,
  so every batch in a step counts.
- **Real Kafka lag.** Spark's `maxOffsetsBehindLatest` is measured against
  the offsets Spark saw when it planned the batch; without
  `maxOffsetsPerTrigger` it is always 0. The job now asks the broker for its
  latest offsets after each batch and subtracts what the batch read.
- **"Kept up" is a rule, not a judgement.** A step kept up when Spark read
  at least 90% of the events sent, the backlog did not climb once the step
  had settled (the first three batches are ignored; after that, the lowest
  backlog of the second half must not be above the highest of the first
  half), and the backlog cleared within two trigger intervals after the load
  stopped. The backlog's slope is noise, so it is never used directly; the
  `KafkaLagGrowing` alert uses the same floor comparison.
- **Latency by probes.** Probe events with unique product ids are sent
  during each step; latency is the time from sending to the row's
  `_updated_at`. Rows are stamped when they are written, not when the batch
  starts: Spark computes a batch lazily, so a start-time stamp read low.
- **Recovery by counting.** The recovery test kills Spark with SIGKILL through
  the Docker Engine API and compares events acknowledged by Kafka with events
  counted in MongoDB, per product. Only its container gets the Docker socket,
  and only when run by hand.

## Rejected
- **A separate benchmarking tool (e.g. Kafka's perf scripts).** They measure
  the broker, not the pipeline.
- **Timing pair probes as throughput latency.** A pair can only be final once
  its window, the co-view gap and the watermark have passed (about 5
  minutes). It is reported separately, as a design property.

## Consequences
The numbers describe one machine where Kafka, Spark, MongoDB and the load
generator share the same cores. They show where this setup saturates and how
it behaves, not what a cluster would do.
