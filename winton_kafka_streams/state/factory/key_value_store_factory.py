from typing import TypeVar, Generic

from winton_kafka_streams.processor.serialization import Serde
from winton_kafka_streams.state.factory.in_memory_key_value_store_factory import InMemoryKeyValueStoreFactory

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class KeyValueStoreFactory(Generic[KT, VT]):
    def __init__(self, name: str, key_serde: Serde[KT], value_serde: Serde[VT]) -> None:
        self.name: str = name
        self.key_serde: Serde[KT] = key_serde
        self.value_serde: Serde[VT] = value_serde

    def in_memory(self) -> InMemoryKeyValueStoreFactory[KT, VT]:
        return InMemoryKeyValueStoreFactory[KT, VT](self.name, self.key_serde, self.value_serde)

    def persistent(self):
        raise NotImplementedError("Persistent State Store not implemented")
