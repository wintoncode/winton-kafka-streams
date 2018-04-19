from typing import Iterator, TypeVar

from winton_kafka_streams.state.key_value_state_store import KeyValueStateStore
from winton_kafka_streams.state.logging.store_change_logger import StoreChangeLogger

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class ChangeLoggingKeyValueStore(KeyValueStateStore[KT, VT]):
    def __init__(self, change_logger: StoreChangeLogger, inner_kv_store: KeyValueStateStore[KT, VT]):
        super(ChangeLoggingKeyValueStore, self).__init__()
        self.change_logger: StoreChangeLogger = change_logger
        self.inner_kv_store: KeyValueStateStore[KT, VT] = inner_kv_store

    def __len__(self) -> int:
        return len(self.inner_kv_store)

    def __iter__(self) -> Iterator[KT]:
        return self.inner_kv_store.__iter__()

    def __setitem__(self, key: KT, value: VT):
        self.inner_kv_store.__setitem__(key, value)
        self.change_logger.log_change(key, value)

    def __getitem__(self, key: KT) -> VT:
        return self.inner_kv_store.__getitem__(key)

    def __delitem__(self, key: KT):
        self.inner_kv_store.__delitem__(key)
        self.change_logger.log_change(key, b'')  # TODO : what is a tombstone?
