"""
Base class for serializer implementations

"""

import abc

from typing import TypeVar, Generic

T = TypeVar('T')


class Serializer(Generic[T], metaclass=abc.ABCMeta):
    """
    Configure this serializer.

    Parameters:
    -----------
    configs : dict
        configs in key/value pairs
    is_key : bool
        whether is for key or value
    """
    @abc.abstractmethod
    def configure(self, configs, is_key):
        pass

    """
    Convert typed data into a bytes.
    
    Parameters:
    -----------
    topic : string
    data : typed data
    
    Returns:
    --------
    serialized_bytearray : bytes
    """
    @abc.abstractmethod
    def serialize(self, topic: str, data: T) -> bytes:
        pass

    """
    Close this serializer.
    This method has to be idempotent because it might be called multiple times.
    """
    @abc.abstractmethod
    def close(self):
        pass
