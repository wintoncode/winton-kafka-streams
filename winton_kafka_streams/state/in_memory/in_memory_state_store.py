from typing import Iterator, TypeVar

from ..key_value_state_store import KeyValueStateStore
from ..state_store import StateStore
from ..state_store_supplier import StateStoreSupplier

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class InMemoryStateStore(StateStore):
    def __init__(self, name, key_serde, value_serde, logging_enabled):
        super().__init__(name, key_serde, value_serde, logging_enabled)
        self.dict = {}

    def initialize(self, context, root):
        pass

    def get_key_value_store(self) -> KeyValueStateStore[KT, VT]:
        parent = self

        class InMemoryKeyValueStateStore(KeyValueStateStore[KT, VT]):
            def __setitem__(self, k: KT, v: VT) -> None:
                parent.dict[k] = v

            def __delitem__(self, v: KT) -> None:
                del parent.dict[v]

            def __getitem__(self, k: KT) -> VT:
                return parent.dict[k]

            def __len__(self) -> int:
                return len(parent.dict)

            def __iter__(self) -> Iterator[KT]:
                return parent.dict.__iter__()

        return InMemoryKeyValueStateStore()


class InMemoryStateStoreSupplier(StateStoreSupplier):
    def __init__(self, name, key_serde, value_serde, logging_enabled):
        super().__init__(name, key_serde, value_serde, logging_enabled)

    def get(self) -> StateStore:
        return InMemoryStateStore(self.name, self._key_serde, self._value_serde, self.logging_enabled)
