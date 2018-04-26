from typing import TypeVar

from winton_kafka_streams.processor.serialization import Serde
from winton_kafka_streams.state.factory.base_storage_key_value_store_factory import BaseStorageKeyValueStoreFactory
from winton_kafka_streams.state.in_memory.in_memory_state_store_supplier import InMemoryStateStoreSupplier

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class InMemoryKeyValueStoreFactory(BaseStorageKeyValueStoreFactory[KT, VT]):
    def __init__(self, name: str, key_serde: Serde[KT], value_serde: Serde[VT]) -> None:
        super(InMemoryKeyValueStoreFactory, self).__init__(name, key_serde, value_serde)

    def build(self) -> InMemoryStateStoreSupplier:
        return InMemoryStateStoreSupplier(self.name, self.key_serde, self.value_serde, self.logging_enabled)
