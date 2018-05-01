"""
Base class for deserializer implementations

"""

import abc

from typing import TypeVar, Generic

from .deserializer import Deserializer
from .serializer import Serializer

T = TypeVar('T')
TSer = TypeVar('TSer')
TDe = TypeVar('TDe')


def extract_config_property(configs, is_key, property_name, default_value=None):
    overridden_property_name = ('KEY_%s' % property_name) if is_key else ('VALUE_%s' % property_name)
    prop_value = getattr(configs, overridden_property_name, None)
    if prop_value is None:
        prop_value = getattr(configs, property_name, default_value)
    return prop_value


class AsymmetricSerde(Generic[TSer, TDe], metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def serializer(self) -> Serializer[TSer]:
        pass

    @property
    @abc.abstractmethod
    def deserializer(self) -> Deserializer[TDe]:
        pass

    @abc.abstractmethod
    def configure(self, configs, is_key):
        pass

    @abc.abstractmethod
    def close(self):
        pass


class Serde(AsymmetricSerde[T, T]):
    """
    Get Serializer

    Returns:
    --------
    serializer : Serializer
    """

    @property
    @abc.abstractmethod
    def serializer(self) -> Serializer[T]:
        pass

    """
    Get Deserializer

    Returns:
    --------
    deserializer : Deserializer
    """

    @property
    @abc.abstractmethod
    def deserializer(self) -> Deserializer[T]:
        pass

    """
    Configure this class, which will configure the underlying serializer and deserializer.

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
    Close this serde class, which will close the underlying serializer and deserializer.
    This method has to be idempotent because it might be called multiple times.
    """

    @abc.abstractmethod
    def close(self):
        pass

