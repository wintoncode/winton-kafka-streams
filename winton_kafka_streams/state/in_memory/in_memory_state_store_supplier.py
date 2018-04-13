from typing import TypeVar

from .in_memory_state_store import InMemoryStateStore
from ..state_store import StateStore
from ..state_store_supplier import StateStoreSupplier

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class InMemoryStateStoreSupplier(StateStoreSupplier):
    def __init__(self, name, key_serde, value_serde, logging_enabled):
        super().__init__(name, key_serde, value_serde, logging_enabled)

    def get(self) -> StateStore:
        return InMemoryStateStore(self.name, self._key_serde, self._value_serde, self.logging_enabled)
