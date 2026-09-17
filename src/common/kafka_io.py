"""
Kafka helpers shared across the project: the serializers every producer uses,
and the consumer-lag reading behind /metrics.

kafka-python 3.x deprecates plain functions as serializers and prints a
DeprecationWarning for each producer it builds - which PowerShell shows as a
red error. These classes implement kafka.serializer.Serializer instead.
"""

import contextlib
import json
import logging
import threading
import time

from kafka.serializer import Serializer


class JsonValueSerializer(Serializer):
    """dict -> JSON bytes. Bytes pass through untouched, which is how the
    deliberately malformed events reach the dead-letter path."""

    def serialize(self, topic, *args):
        # kafka-python 3.x calls serialize(topic, headers, data);
        # 2.x calls serialize(topic, data). The payload is always last.
        data = args[-1]
        if data is None or isinstance(data, (bytes, bytearray)):
            return data
        return json.dumps(data).encode("utf-8")


class StringKeySerializer(Serializer):
    """Any key -> UTF-8 bytes of its string form."""

    def serialize(self, topic, *args):
        key = args[-1]
        return None if key is None else str(key).encode("utf-8")


# --------------------------------------------------------------- consumer lag
#
# Spark's own Kafka metric (maxOffsetsBehindLatest) compares what a batch read
# with the latest offsets Spark saw WHEN IT PLANNED that batch. Without
# maxOffsetsPerTrigger a batch always reads everything it saw, so the metric
# is 0 by construction - it stayed at exactly 0.0 for hours, even while
# batches took seconds. Real lag is the gap between the broker's latest
# offsets NOW and what the batch finished reading.

_LAG_LOCK = threading.Lock()
_LAG_CONSUMER = None
_LAG_RETRY_AT = 0.0          # after a failure, wait before asking again
LAG_RETRY_SECONDS = 30
log = logging.getLogger("kafka_io")

# Checked against KafkaConsumer.DEFAULT_CONFIG by a test: kafka-python rejects
# unknown keys, and 3.x renamed api_version_auto_timeout_ms to
# bootstrap_timeout_ms - using the old name made every lag reading fail.
LAG_CONSUMER_CONFIG = {
    "enable_auto_commit": False,
    "client_id": "affinity-lag-reader",
    "bootstrap_timeout_ms": 5000,
    "request_timeout_ms": 10000,
}


def latest_offsets(topic, bootstrap_servers):
    """{partition: latest offset} from the broker, or None if unavailable."""
    global _LAG_CONSUMER, _LAG_RETRY_AT
    with _LAG_LOCK:
        if time.time() < _LAG_RETRY_AT:
            return None          # a failed attempt blocks for seconds; back off
        try:
            if _LAG_CONSUMER is None:
                from kafka import KafkaConsumer
                _LAG_CONSUMER = KafkaConsumer(
                    bootstrap_servers=bootstrap_servers, **LAG_CONSUMER_CONFIG)
            partitions = _LAG_CONSUMER.partitions_for_topic(topic)
            if not partitions:
                return None
            from kafka import TopicPartition
            ends = _LAG_CONSUMER.end_offsets(
                [TopicPartition(topic, p) for p in sorted(partitions)],
                timeout_ms=5000)
            return {tp.partition: int(offset) for tp, offset in ends.items()}
        except Exception as exc:                          # noqa: BLE001
            log.warning("could not read latest Kafka offsets (retrying in "
                        "%ss): %s", LAG_RETRY_SECONDS, exc)
            _LAG_RETRY_AT = time.time() + LAG_RETRY_SECONDS
            if _LAG_CONSUMER is not None:
                with contextlib.suppress(Exception):
                    _LAG_CONSUMER.close()
            _LAG_CONSUMER = None
            return None


def offsets_behind(end_offset, latest, topic):
    """
    Events not yet read: sum over partitions of (latest - read up to).

    `end_offset` is a Spark source's endOffset - a JSON string such as
    '{"clickstream":{"0":120,"1":98}}' - or an already-parsed dict.
    Returns None when either side is unknown.
    """
    if not latest or end_offset in (None, "", "null"):
        return None
    try:
        data = json.loads(end_offset) if isinstance(end_offset, str) else end_offset
        read = (data or {}).get(topic)
    except (TypeError, ValueError, AttributeError):
        return None
    if not read:
        return None
    behind = 0
    for partition, newest in latest.items():
        done = read.get(str(partition), read.get(partition))
        if done is None:
            return None
        behind += max(int(newest) - int(done), 0)
    return float(behind)
