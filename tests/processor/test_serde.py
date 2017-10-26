"""
Test of all serde classes

"""

from winton_kafka_streams.processor.serialization.serdes import IdentitySerde


def test_identity_serde():
    identity_serde = IdentitySerde()
    assert identity_serde.serializer.serialise(123) == 123
    assert identity_serde.serializer.serialise('hello') == 'hello'

    assert identity_serde.deserializer.deserialise(123) == 123
    assert identity_serde.deserializer.deserialise('hello') == 'hello'

