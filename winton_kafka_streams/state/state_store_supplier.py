from abc import ABC, abstractmethod

from typing import TypeVar, Generic, Callable

from .state_store import StateStore
from ..processor.serialization import Serde

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class StateStoreSupplier(ABC, Generic[KT, VT]):
    """
    StateStoreSuppliers are added to a topology and are accessible from each StreamThread

    """

    def __init__(self, name: str, key_serde: Serde[KT], value_serde, logging_enabled):
        self.logging_enabled = logging_enabled
        self._value_serde = value_serde
        self._key_serde = key_serde
        self.name = name

    @abstractmethod
    def get(self) -> StateStore[KT, VT]:
        """Create a StateStore for each StreamTask. *These StateStores may exist in different threads.*"""
        pass
