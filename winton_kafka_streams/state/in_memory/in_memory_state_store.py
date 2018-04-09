from typing import Iterator, TypeVar

from winton_kafka_streams.state.key_value_state_store import KeyValueStateStore
from winton_kafka_streams.state.state_store import StateStore

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class InMemoryStateStore(StateStore):
    def __init__(self, name):
        super().__init__(name)
        self.dict = {}

    def initialize(self, context, root):
        # TODO: register with context, passing restore callback
        pass

    def get_key_value_store(self) -> KeyValueStateStore[KT, VT]:
        class InMemoryKeyValueStateStore(KeyValueStateStore[KT, VT]):
            def get(self, k: KT, default: VT = None):
                return self.parent.dict.get(k, default)

            def __init__(self, parent):
                self.parent = parent

            def __setitem__(self, k: KT, v: VT) -> None:
                self.parent.dict[k] = v

            def __delitem__(self, v: KT) -> None:
                del self.parent.dict[v]

            def __getitem__(self, k: KT) -> VT:
                return self.parent.dict[k]

            def __len__(self) -> int:
                return len(self.parent.dict)

            def __iter__(self) -> Iterator[KT]:
                return self.parent.dict.__iter__()

        return InMemoryKeyValueStateStore(self)
