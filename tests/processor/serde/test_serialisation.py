from winton_kafka_streams.processor.serialization.serdes import IdentitySerde


def test_identity_serde():
    identity_serde = IdentitySerde()
    assert identity_serde.serializer.serialize('topic', 123) == 123
    assert identity_serde.serializer.serialize('topic', 'hello') == 'hello'

    assert identity_serde.deserializer.deserialize('topic', 123) == 123
    assert identity_serde.deserializer.deserialize('topic', 'hello') == 'hello'
