from confluent_kafka.avro import CachedSchemaRegistryClient, MessageSerializer
from confluent_kafka.avro import loads as avro_loads

from .serde import extract_config_property
from .deserializer import Deserializer
from .serializer import Serializer


class AvroHelper:
    def __init__(self):
        self._is_key = False
        self._schema_registry = None
        self._serializer = None
        self._schema = None

    def _set_serializer(self, schema_registry):
        self._schema_registry = schema_registry
        self._serializer = MessageSerializer(registry_client=self._schema_registry)

    def configure(self, configs, is_key):
        self._is_key = is_key
        schema_registry_url = extract_config_property(configs, is_key, 'AVRO_SCHEMA_REGISTRY')
        schema = extract_config_property(configs, is_key, 'AVRO_SCHEMA')

        if schema_registry_url is None:
            raise Exception("Missing Avro Schema Registry Url")
        else:
            self._set_serializer(CachedSchemaRegistryClient(url=schema_registry_url))

        if schema:
            self._schema = avro_loads(schema)

    def serialize(self, topic, data):
        if self._schema is None:
            raise Exception("Missing Avro Schema")

        return self._serializer.encode_record_with_schema(topic, self._schema, data, is_key=self._is_key)

    def deserialize(self, topic, data):
        return self._serializer.decode_message(data)


class AvroSerializer(Serializer):
    def __init__(self):
        self._avro_helper = AvroHelper()

    def serialize(self, topic, data):
        return self._avro_helper.serialize(topic, data)

    def configure(self, configs, is_key):
        self._avro_helper.configure(configs, is_key)

    def close(self):
        pass


class AvroDeserializer(Deserializer):
    def __init__(self):
        self._avro_helper = AvroHelper()

    def deserialize(self, topic, data):
        return self._avro_helper.deserialize(topic, data)

    def configure(self, configs, is_key):
        self._avro_helper.configure(configs, is_key)

    def close(self):
        pass
