from typing import Generic, TypeVar

from winton_kafka_streams.processor.serialization import Serde
from abc import ABC, abstractmethod

from winton_kafka_streams.state.state_store_supplier import StateStoreSupplier

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class BaseStorageKeyValueStoreFactory(ABC, Generic[KT, VT]):
    def __init__(self, name: str, key_serde: Serde[KT], value_serde: Serde[VT]) -> None:
        self.name: str = name
        self.key_serde: Serde[KT] = key_serde
        self.value_serde: Serde[VT] = value_serde
        self.logging_enabled: bool = True

    def enable_logging(self, config_map):
        # TODO changelog extra config gets handled here
        self.logging_enabled = True
        return self

    def disable_logging(self):
        self.logging_enabled = False
        return self

    @abstractmethod
    def build(self) -> StateStoreSupplier[KT, VT]:
        pass
