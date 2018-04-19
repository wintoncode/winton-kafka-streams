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
        self.serializer = serializer
        self.deserializer = deserializer

    def configure(self, configs, is_key):
        self.serializer.configure(configs, is_key)
        self.deserializer.configure(configs, is_key)

    def serializer(self) -> Serializer[TSer]:
        return self.serializer

    def deserializer(self) -> Deserializer[TDe]:
        return self.deserializer

    def close(self):
        self.serializer.close()
        self.deserializer.close()


class WrapperSerde(AsymmetricWrapperSerde[T, T], Serde[T]):
    pass
