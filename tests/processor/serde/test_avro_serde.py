import io
import struct
from confluent_kafka.avro import loads as avro_loads
from .mock_schema_registry import MockSchemaRegistryClient
from winton_kafka_streams.processor.serialization.serdes import AvroSerde
import winton_kafka_streams.kafka_config as config

string_avro = '{"type": "string"}'


def create_serde(registry, schema):
    serde = AvroSerde()
    config.AVRO_SCHEMA_REGISTRY = 'nowhere'
    config.KEY_AVRO_SCHEMA = schema

    serde.configure(config, True)
    serde.serializer._avro_helper._set_serializer(registry)
    serde.deserializer._avro_helper._set_serializer(registry)

    serde.test_registry = registry
    return serde


def test_serialize_avro():
    registry = MockSchemaRegistryClient()
    serde = create_serde(registry, string_avro)

    message = serde.serializer.serialize('topic', 'data')
    message_io = io.BytesIO(message)
    magic, schema_id, length, string = struct.unpack('>bIb4s', message_io.read(10))
    assert(0 == magic)
    assert(schema_id in registry.id_to_schema)
    assert(8 == length)  # (==4) uses variable-length zig-zag encoding
    assert(b'data' == string)
    message_io.close()


def test_deserialize_avro():
    registry = MockSchemaRegistryClient()
    serde = create_serde(registry, string_avro)
    schema_id = registry.register('topic-value', avro_loads(string_avro))

    serialized = b'\0' + schema_id.to_bytes(4, 'big') + b'\x08data'
    message = serde.deserializer.deserialize('ignored', serialized)
    assert('data' == message)
