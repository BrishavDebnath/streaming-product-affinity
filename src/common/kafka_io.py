"""
Kafka serializers shared by every producer in the project.

kafka-python 3.x deprecates plain functions as serializers and prints a
DeprecationWarning for each producer it builds - which PowerShell shows as a
red error. These classes implement kafka.serializer.Serializer instead.
"""

import json

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
