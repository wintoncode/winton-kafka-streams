from winton_kafka_streams.processor.serialization.serdes import StringSerde, IntegerSerde, LongSerde, DoubleSerde, \
    BytesSerde


class ValueStoreFactory:
    def __init__(self, name, key_serde):
        self.name = name
        self.key_serde = key_serde
        self.value_serde = None

    def _with_value_serde(self, serde):
        self.value_serde = serde
        configs = None  # TODO
        is_key = False
        self.value_serde.configure(configs, is_key)
        return self

    def with_string_values(self):
        return self._with_value_serde(StringSerde())

    def with_integer_values(self):
        return self._with_value_serde(IntegerSerde())

    def with_long_values(self):
        return self._with_value_serde(LongSerde())

    def with_double_values(self):
        return self._with_value_serde(DoubleSerde())

    def with_byte_values(self):
        return self._with_value_serde(BytesSerde())
