from winton_kafka_streams.processor.serialization.serdes import BytesSerde


def test_bytearray_serde():
    bytearray_serde = BytesSerde()
    assert bytearray_serde.serializer.serialize('topic', 123) == 123
    assert bytearray_serde.serializer.serialize('topic', 'hello') == 'hello'

    assert bytearray_serde.deserializer.deserialize('topic', 123) == 123
    assert bytearray_serde.deserializer.deserialize('topic', 'hello') == 'hello'
