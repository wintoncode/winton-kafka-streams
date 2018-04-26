from typing import TypeVar

from winton_kafka_streams.processor.serialization import Serde
from winton_kafka_streams.processor.serialization.serdes import *
from winton_kafka_streams.state.factory.value_store_factory import ValueStoreFactory

KT = TypeVar('KT')  # Key type.


class StoreFactory:
    def __init__(self, name: str) -> None:
        self.name: str = name

    def _with_key_serde(self, serde: Serde[KT]) -> ValueStoreFactory[KT]:
        key_serde: Serde[KT] = serde
        configs = None  # TODO
        is_key = True
        key_serde.configure(configs, is_key)
        return ValueStoreFactory[KT](self.name, key_serde)

    def with_string_keys(self) -> ValueStoreFactory[str]:
        return self._with_key_serde(StringSerde())

    def with_integer_keys(self) -> ValueStoreFactory[int]:
        return self._with_key_serde(IntegerSerde())

    def with_long_keys(self) -> ValueStoreFactory[int]:
        return self._with_key_serde(LongSerde())

    def with_double_keys(self) -> ValueStoreFactory[float]:
        return self._with_key_serde(DoubleSerde())

    def with_bytes_keys(self) -> ValueStoreFactory[bytes]:
        return self._with_key_serde(BytesSerde())
