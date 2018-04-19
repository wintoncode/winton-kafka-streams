from abc import ABC, abstractmethod

from typing import TypeVar, Generic

from .state_store import StateStore
from ..processor.serialization import Serde

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class StateStoreSupplier(ABC, Generic[KT, VT]):
    """
    StateStoreSuppliers are added to a topology and are accessible from each StreamThread

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
    def get(self) -> StateStore[KT, VT]:
        """Create a StateStore for each StreamTask. *These StateStores may exist in different threads.*"""
        pass
