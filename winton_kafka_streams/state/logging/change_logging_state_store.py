from typing import TypeVar

from ..key_value_state_store import KeyValueStateStore
from ..logging.change_logging_key_value_store import ChangeLoggingKeyValueStore
from ..state_store import StateStore
from .store_change_logger import StoreChangeLogger

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class ChangeLoggingStateStore(StateStore[KT, VT]):
    def __init__(self, name, inner_state_store: StateStore[KT, VT]):
        super().__init__(name)
        self.inner_state_store = inner_state_store
        self.change_logger = None

    def initialize(self, context, root):
        self.inner_state_store.initialize(context, root)
        self.change_logger = StoreChangeLogger(self.inner_state_store.name, context)
        # TODO rebuild state into inner here

    def get_key_value_store(self) -> KeyValueStateStore[KT, VT]:
        return ChangeLoggingKeyValueStore(self.change_logger, self.inner_state_store.get_key_value_store())
