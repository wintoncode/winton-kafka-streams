from typing import TypeVar, Iterator

from collections.abc import MutableMapping
from abc import abstractmethod

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class KeyValueStateStore(MutableMapping[KT, VT]):
    @abstractmethod
    def __setitem__(self, k: KT, v: VT) -> None:
        pass

    @abstractmethod
    def __delitem__(self, v: KT) -> None:
        pass

    @abstractmethod
    def __getitem__(self, k: KT) -> VT:
        pass

    @abstractmethod
    def __len__(self) -> int:
        pass

    @abstractmethod
    def __iter__(self) -> Iterator[KT]:
        pass

    @abstractmethod
    def get(self, k: KT, default: VT):
        pass
