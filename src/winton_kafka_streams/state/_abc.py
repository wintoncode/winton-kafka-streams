"""
Abstract classes for implementations of state classes

"""

import abc

class StateBase(metaclass=abc.ABCMeta):
    """
    Interface that must be implemented by all state classes

    """

    @abc.abstractmethod
    def add_result(self, v):
        pass

    @abc.abstractmethod
    def has_data(self):
        pass