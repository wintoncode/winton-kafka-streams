"""
Abstract class for implementations of state store supplier classes

"""

from abc import ABC, abstractmethod

from winton_kafka_streams.state.state_store import StateStore


class StateStoreSupplier(ABC):
    """
    Interface that must be implemented by all state store suppliers

    """

    def __init__(self, name, key_serde, value_serde, logging_enabled):
        self.logging_enabled = logging_enabled
        self._value_serde = value_serde
        self._key_serde = key_serde
        self.name = name

    @abstractmethod
    def get(self) -> StateStore:
        pass
