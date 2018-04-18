from typing import TypeVar, Generic

from winton_kafka_streams.state.factory.in_memory_key_value_store_factory import InMemoryKeyValueStoreFactory

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class KeyValueStoreFactory(Generic[KT, VT]):
    def __init__(self, name, key_serde, value_serde):
        self.name = name
        self.key_serde = key_serde
        self.value_serde = value_serde

    def in_memory(self) -> InMemoryKeyValueStoreFactory[KT, VT]:
        return InMemoryKeyValueStoreFactory(self.name, self.key_serde, self.value_serde)

    def persistent(self):
        raise NotImplementedError("Persistent State Store not implemented")
