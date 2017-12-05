from confluent_kafka.avro import CachedSchemaRegistryClient
from confluent_kafka.avro import loads as avro_loads

from ._serde import extract_config_property
from ._deserializer import Deserializer
from ._serializer import Serializer
import struct


def _configure(self, configs, is_key):
    schema_registry_url = extract_config_property(configs, is_key, 'AVRO_SCHEMA_REGISTRY')
    key_schema = extract_config_property(configs, is_key, 'AVRO_KEY_SCHEMA')
    value_schema = extract_config_property(configs, is_key, 'AVRO_VALUE_SCHEMA')

    if schema_registry_url is None:
        raise Exception("Missing Avro Schema Registry Url")
    else:
        self._schema_registry = CachedSchemaRegistryClient(url=schema_registry_url)

    if key_schema is None:
        raise Exception("Missing Avro Key Schema")
    else:
        self._key_schema = avro_loads(key_schema)

    if value_schema is None:
        raise Exception("Missing Avro Value Schema")
    else:
        self._value_schema = avro_loads(value_schema)


class AvroSerializer(Serializer):
    def __init__(self):
        self._schema_registry = None
        self._key_schema = None
        self._value_schema = None

    def serialize(self, topic, data):
        return struct.pack('f', data)

    def configure(self, configs, is_key):
        _configure(self, configs, is_key)

    def close(self):
        pass


class AvroDeserializer(Deserializer):
    def __init__(self):
        self._schema_registry = None
        self._key_schema = None
        self._value_schema = None

    def deserialize(self, topic, data):
        return struct.unpack('f', data)[0]

    def configure(self, configs, is_key):
        _configure(self, configs, is_key)

    def close(self):
        pass
