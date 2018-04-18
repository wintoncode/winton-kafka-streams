from typing import TypeVar, Iterator, MutableMapping


from abc import abstractmethod

KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.


class KeyValueStateStore(MutableMapping[KT, VT]):
    """
    Dict-like class is injected into a processors to provide an interface to the underlying StateStore
    """
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
