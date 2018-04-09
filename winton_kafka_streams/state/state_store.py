from abc import ABC, abstractmethod
from typing import TypeVar, Generic

from winton_kafka_streams.state.key_value_state_store import KeyValueStateStore

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class StateStore(ABC, Generic[KT, VT]):
    def __init__(self, name):
        self.name = name

    @abstractmethod
    def initialize(self, context, root):
        pass

    @abstractmethod
    def get_key_value_store(self) -> KeyValueStateStore[KT, VT]:
        pass
