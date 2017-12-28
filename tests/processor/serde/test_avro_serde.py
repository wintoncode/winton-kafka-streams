from .mock_schema_registry import MockSchemaRegistryClient
from winton_kafka_streams.processor.serialization.serdes import AvroSerde
import winton_kafka_streams.kafka_config as Config


def test_serialize_avro():
    registry = MockSchemaRegistryClient()
    serde = AvroSerde()
    Config.AVRO_SCHEMA_REGISTRY = 'http://localhost:9000'
    Config.KEY_AVRO_SCHEMA = '{"type": "string"}'

    serde.configure(Config, True)
    serde.serializer._avro_helper._schema_registry = registry
    message = serde.serializer.serialize('topic', 'data')
    assert('', message)
