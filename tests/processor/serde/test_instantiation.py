import winton_kafka_streams.processor.serialization.serdes as serdes


def test_serde_instance_to_string():
    serde = serdes.BytesSerde()
    serde_str = serdes.serde_as_string(serde)
    assert 'winton_kafka_streams.processor.serialization.serdes.bytes_serde.BytesSerde' == serde_str


def test_serde_class_to_string():
    serde = serdes.BytesSerde
    serde_str = serdes.serde_as_string(serde)
    assert 'winton_kafka_streams.processor.serialization.serdes.bytes_serde.BytesSerde' == serde_str


def test_string_to_serde():
    serde_str = 'winton_kafka_streams.processor.serialization.serdes.StringSerde'
    serde = serdes.serde_from_string(serde_str)
    byte_str = serde.serializer.serialize('topic', 'abc123')
    assert b'abc123' == byte_str
