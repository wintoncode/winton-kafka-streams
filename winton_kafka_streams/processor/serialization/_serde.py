"""
Base class for deserializer implementations

"""

import abc


def extract_config_property(configs, is_key, property_name):
    overridden_property_name = ('KEY_%s' % property_name) if is_key else ('VALUE_%s' % property_name)
    prop_value = getattr(configs, overridden_property_name, None)
    if prop_value is None:
        prop_value = getattr(configs, property_name)
    return prop_value


class Serde(metaclass=abc.ABCMeta):
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
    Get Serializer

    Returns:
    --------
    serializer : Serializer
    """

    @abc.abstractmethod
    def serializer(self):
        pass

    """
    Get Deserializer

    Returns:
    --------
    deserializer : Deserializer
    """

    @abc.abstractmethod
    def deserializer(self):
        pass

    """
    Close this serde class, which will close the underlying serializer and deserializer.
    This method has to be idempotent because it might be called multiple times.
    """

    @abc.abstractmethod
    def close(self):
        pass
