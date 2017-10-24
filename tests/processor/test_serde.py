"""
Test of all serde classes

"""

import winton_kafka_streams.processor.serialization.serdes.identity as identity


def test_identitySerde():
    ident_serde = identity.IdentitySerde()
    assert ident_serde.serialise(123) == 123
    assert ident_serde.serialise('hello') == 'hello'

    assert ident_serde.deserialise(123) == 123
    assert ident_serde.deserialise('hello') == 'hello'

