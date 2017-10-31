"""
Base class for deserializer implementations

"""

import abc


def extract_config_property(configs, is_key, property_name):
    prop_value = ''
    overridden_property_name = ('key.%s' % property_name) if is_key else ('value.%s' % property_name)
    if overridden_property_name in configs:
        prop_value = configs[overridden_property_name]
    elif property_name in configs:
        prop_value = configs[property_name]
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
