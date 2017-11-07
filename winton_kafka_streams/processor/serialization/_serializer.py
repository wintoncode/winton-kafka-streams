"""
Base class for serializer implementations

"""

import abc


class Serializer(metaclass=abc.ABCMeta):
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
    Convert typed data into a bytearray.
    
    Parameters:
    -----------
    topic : string
    data : typed data
    
    Returns:
    --------
    serialized_bytearray : bytearray
    """
    @abc.abstractmethod
    def serialize(self, topic, data):
        pass

    """
    Close this serializer.
    This method has to be idempotent because it might be called multiple times.
    """
    @abc.abstractmethod
    def close(self):
        pass
