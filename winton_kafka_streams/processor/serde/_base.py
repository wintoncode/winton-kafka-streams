"""
Base class for serde implementations

"""

import abc


class BaseSerde(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def serialise(self, value):
        pass

    @abc.abstractmethod
    def deserialise(self, value):
        pass
