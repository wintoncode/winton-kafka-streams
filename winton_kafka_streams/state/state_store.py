from abc import ABC, abstractmethod
from typing import TypeVar, Generic

from .key_value_state_store import KeyValueStateStore

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class StateStore(ABC, Generic[KT, VT]):
    """
    StateStores are created by Suppliers for use in StreamTasks
    """
    def __init__(self, name, key_serde, value_serde, logging_enabled):
        self.logging_enabled = logging_enabled
        self._value_serde = value_serde
        self._key_serde = key_serde
        self.name = name

    @abstractmethod
    def initialize(self, context, root):
        """
        Initialize is called within a StreamTask once partitions are assigned, before processing starts.
        State is rebuilt from the change log at this point.
        :param context:
        :param root:
        :return:  None
        """
        pass

    @abstractmethod
    def get_key_value_store(self) -> KeyValueStateStore[KT, VT]:
        pass
