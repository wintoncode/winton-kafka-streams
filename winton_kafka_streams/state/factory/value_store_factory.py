from typing import TypeVar, Generic

from winton_kafka_streams.processor.serialization import Serde
from winton_kafka_streams.processor.serialization.serdes import *
from .key_value_store_factory import KeyValueStoreFactory

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class ValueStoreFactory(Generic[KT]):
    def __init__(self, name: str, key_serde: Serde[KT]) -> None:
        self.name: str = name
        self.key_serde: Serde[KT] = key_serde

    def _with_value_serde(self, serde: Serde[VT]) -> KeyValueStoreFactory[KT, VT]:
        value_serde: Serde[VT] = serde
        configs = None
        is_key = False
        value_serde.configure(configs, is_key)
        return KeyValueStoreFactory[KT, VT](self.name, self.key_serde, value_serde)

    def with_string_values(self) -> KeyValueStoreFactory[KT, str]:
        return self._with_value_serde(StringSerde())

    def with_integer_values(self) -> KeyValueStoreFactory[KT, int]:
        return self._with_value_serde(IntegerSerde())

    def with_long_values(self) -> KeyValueStoreFactory[KT, int]:
        return self._with_value_serde(LongSerde())

    def with_double_values(self) -> KeyValueStoreFactory[KT, float]:
        return self._with_value_serde(DoubleSerde())

    def with_bytes_values(self) -> KeyValueStoreFactory[KT, bytes]:
        return self._with_value_serde(BytesSerde())
