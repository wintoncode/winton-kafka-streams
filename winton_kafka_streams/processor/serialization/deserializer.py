"""
Base class for deserializer implementations

"""

import abc

from typing import TypeVar, Generic

T = TypeVar('T')


class Deserializer(Generic[T], metaclass=abc.ABCMeta):
    """
    Configure this deserializer.

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
    Convert a bytes into typed data.
    
    Parameters:
    -----------
    topic : string
    data : bytes
    
    Returns:
    --------
    deserialized_data : typed data
    """
    @abc.abstractmethod
    def deserialize(self, topic: str, data: bytes) -> T:
        pass

    """
    Close this deserializer.
    This method has to be idempotent because it might be called multiple times.
    """
    @abc.abstractmethod
    def close(self):
        pass
