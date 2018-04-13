from abc import ABC, abstractmethod

from .state_store import StateStore


class StateStoreSupplier(ABC):
    """
    StateStoreSuppliers are added to a topology and are accessible from each StreamThread

    """

    def __init__(self, name, key_serde, value_serde, logging_enabled):
        self.logging_enabled = logging_enabled
        self._value_serde = value_serde
        self._key_serde = key_serde
        self.name = name

    @abstractmethod
    def get(self) -> StateStore:
        """Create a StateStore for each StreamTask. *These StateStores may exist in different threads.*"""
        pass
