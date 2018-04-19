from abc import ABC, abstractmethod
from typing import TypeVar, Generic

from ..processor.serialization import Serde
from .key_value_state_store import KeyValueStateStore

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class StateStore(ABC, Generic[KT, VT]):
    """
    StateStores are created by Suppliers for use in StreamTasks
    """
    def __init__(self, name: str, key_serde: Serde[KT], value_serde: Serde[VT], logging_enabled: bool):
        self.logging_enabled: bool = logging_enabled
        self._value_serde: Serde[VT] = value_serde
        self._key_serde: Serde[KT] = key_serde
        self._name: str = name

    @property
    def name(self) -> str:
        return self._name

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
