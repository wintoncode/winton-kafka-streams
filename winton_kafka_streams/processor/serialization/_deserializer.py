"""
Base class for deserializer implementations

"""

import abc


class Deserializer(metaclass=abc.ABCMeta):
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
    Convert a bytearray into typed data.
    
    Parameters:
    -----------
    topic : string
    data : bytearray
    
    Returns:
    --------
    deserialized_data : typed data
    """
    @abc.abstractmethod
    def deserialize(self, topic, data):
        pass

    """
    Close this deserializer.
    This method has to be idempotent because it might be called multiple times.
    """
    @abc.abstractmethod
    def close(self):
        pass
