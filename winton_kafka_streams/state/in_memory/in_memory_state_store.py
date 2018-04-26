from typing import Iterator, TypeVar, MutableMapping

from winton_kafka_streams.processor.serialization import Serde
from ..key_value_state_store import KeyValueStateStore
from ..state_store import StateStore

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class InMemoryStateStore(StateStore[KT, VT]):
    def __init__(self,  name: str, key_serde: Serde[KT], value_serde: Serde[VT], logging_enabled: bool) -> None:
        super().__init__(name, key_serde, value_serde, logging_enabled)
        self.dict: MutableMapping[KT, VT] = {}

    def initialize(self, context, root) -> None:
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
