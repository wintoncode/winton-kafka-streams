from abc import ABC, abstractmethod

from typing import TypeVar, Generic

from winton_kafka_streams.state.logging.change_logging_state_store import ChangeLoggingStateStore
from .state_store import StateStore
from ..processor.serialization import Serde

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class StateStoreSupplier(ABC, Generic[KT, VT]):
    """
    StateStoreSuppliers are added to a topology and are accessible from each StreamThread

    """

    def __init__(self, name: str, key_serde: Serde[KT], value_serde: Serde[VT], logging_enabled: bool) -> None:
        self.logging_enabled: bool = logging_enabled
        self._value_serde: Serde[VT] = value_serde
        self._key_serde: Serde[KT] = key_serde
        self._name: str = name

    @property
    def name(self) -> str:
        return self._name

    @abstractmethod
    def _build_state_store(self) -> StateStore[KT, VT]:
        pass

    def get(self) -> StateStore[KT, VT]:
        """Create a StateStore for each StreamTask. *These StateStores may exist in different threads.*"""
        inner = self._build_state_store()
        if self.logging_enabled:
            return ChangeLoggingStateStore(self.name, self._key_serde, self._value_serde, self.logging_enabled, inner)
        else:
            return inner
