from winton_kafka_streams.processor.serialization.serdes import StringSerde, IntegerSerde, LongSerde, DoubleSerde, \
    BytesSerde
from winton_kafka_streams.state.factory.value_store_factory import ValueStoreFactory


class StoreFactory:
    def __init__(self, name):
        self.name = name

    def _with_key_serde(self, serde):
        key_serde = serde
        configs = None  # TODO
        is_key = True
        key_serde.configure(configs, is_key)
        return ValueStoreFactory(self.name, key_serde)

    def with_string_keys(self):
        return self._with_key_serde(StringSerde())

    def with_integer_keys(self):
        return self._with_key_serde(IntegerSerde())

    def with_long_keys(self):
        return self._with_key_serde(LongSerde())

    def with_double_keys(self):
        return self._with_key_serde(DoubleSerde())

    def with_byte_keys(self):
        return self._with_key_serde(BytesSerde())
