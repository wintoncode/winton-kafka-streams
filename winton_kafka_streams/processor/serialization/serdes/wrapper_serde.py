"""
Serde from a Serializer and Deserializer

"""
from typing import TypeVar

from ..deserializer import Deserializer
from ..serializer import Serializer
from ..serde import AsymmetricSerde, Serde

TSer = TypeVar('TSer')
TDe = TypeVar('TDe')


class AsymmetricWrapperSerde(AsymmetricSerde[TSer, TDe]):
    def __init__(self, serializer: Serializer[TSer], deserializer: Deserializer[TDe]) -> None:
        self._serializer = serializer
        self._deserializer = deserializer

    @property
    def serializer(self) -> Serializer[TSer]:
        return self._serializer

    @property
    def deserializer(self) -> Deserializer[TDe]:
        return self._deserializer

    def configure(self, configs, is_key) -> None:
        self.serializer.configure(configs, is_key)
        self.deserializer.configure(configs, is_key)

    def close(self) -> None:
        self.serializer.close()
        self.deserializer.close()


T = TypeVar('T')


class WrapperSerde(Serde[T]):
    def __init__(self, serializer: Serializer[T], deserializer: Deserializer[T]) -> None:
        self._serializer = serializer
        self._deserializer = deserializer

    @property
    def serializer(self) -> Serializer[T]:
        return self._serializer

    @property
    def deserializer(self) -> Deserializer[T]:
        return self._deserializer

    def configure(self, configs, is_key) -> None:
        self.serializer.configure(configs, is_key)
        self.deserializer.configure(configs, is_key)

    def close(self) -> None:
        self.serializer.close()
        self.deserializer.close()
