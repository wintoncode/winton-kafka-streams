"""
Serde from a Serializer and Deserializer

"""
from typing import TypeVar

from .._deserializer import Deserializer
from .._serializer import Serializer
from ..serde import AsymmetricSerde, Serde

TSer = TypeVar('TSer')
TDe = TypeVar('TDe')
T = TypeVar('T')


class AsymmetricWrapperSerde(AsymmetricSerde[TSer, TDe]):
    def __init__(self, serializer: Serializer[TSer], deserializer: Deserializer[TDe]):
        self._serializer = serializer
        self._deserializer = deserializer

    @property
    def serializer(self) -> Serializer[TSer]:
        return self._serializer

    @property
    def deserializer(self) -> Deserializer[TDe]:
        return self._deserializer

    def configure(self, configs, is_key):
        self.serializer.configure(configs, is_key)
        self.deserializer.configure(configs, is_key)

    def close(self):
        self.serializer.close()
        self.deserializer.close()


class WrapperSerde(AsymmetricWrapperSerde[T, T], Serde[T]):
    pass
