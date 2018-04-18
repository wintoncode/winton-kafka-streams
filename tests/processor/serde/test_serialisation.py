from winton_kafka_streams.processor.serialization.serdes import *


def test_bytes_serde():
    bytes_serde = BytesSerde()
    assert bytes_serde.serializer.serialize('topic', b'hello') == b'hello'
    assert bytes_serde.deserializer.deserialize('topic', b'hello') == b'hello'


def test_string_serde():
    string_serde = StringSerde()
    assert string_serde.serializer.serialize('topic', 'hello') == b'hello'
    assert string_serde.deserializer.deserialize('topic', b'hello') == 'hello'


def test_integer_serde():
    int_serde = IntegerSerde()
    assert int_serde.serializer.serialize('topic', -2132) == b'\xac\xf7\xff\xff'
    assert int_serde.deserializer.deserialize('topic', b'\xac\xf7\xff\xff') == -2132


def test_long_serde():
    int_serde = LongSerde()
    assert int_serde.serializer.serialize('topic', -2132) == b'\xac\xf7\xff\xff\xff\xff\xff\xff'
    assert int_serde.deserializer.deserialize('topic', b'\xac\xf7\xff\xff\xff\xff\xff\xff') == -2132


def test_float_serde():
    float_serde = FloatSerde()
    assert float_serde.serializer.serialize('topic', -18.125) == b'\x00\x00\x91\xc1'
    assert float_serde.deserializer.deserialize('topic', b'\x00\x00\x91\xc1') == -18.125


def test_double_serde():
    double_serde = DoubleSerde()
    assert double_serde.serializer.serialize('topic', 123.25) == b'\x00\x00\x00\x00\x00\xd0^@'
    assert double_serde.deserializer.deserialize('topic', b'\x00\x00\x00\x00\x00\xd0^@') == 123.25


def test_json_serde():
    json_serde = JsonSerde()
    test_dict = {'key1': 'val1', 'key2': ["val21", "val22"]}
    test_bytes = b'{"key1": "val1", "key2": ["val21", "val22"]}'
    assert json_serde.serializer.serialize('topic', test_dict) == test_bytes
    assert json_serde.deserializer.deserialize('topic', test_bytes) == test_dict
