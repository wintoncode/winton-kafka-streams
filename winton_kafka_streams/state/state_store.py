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
    def __init__(self, name: str, key_serde: Serde[KT], value_serde: Serde[VT], logging_enabled: bool) -> None:
        self.logging_enabled: bool = logging_enabled
        self._value_serde: Serde[VT] = value_serde
        self._key_serde: Serde[KT] = key_serde
        self._name: str = name

    @property
    def name(self) -> str:
        return self._name

    def serialize_key(self, key: KT) -> bytes:
        return self._key_serde.serializer.serialize("", key)

    def deserialize_key(self, data: bytes) -> KT:
        return self._key_serde.deserializer.deserialize("", data)

    def serialize_value(self, value: VT) -> bytes:
        return self._value_serde.serializer.serialize("", value)

    def deserialize_value(self, data: bytes) -> VT:
        return self._value_serde.deserializer.deserialize("", data)

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
