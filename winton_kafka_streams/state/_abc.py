"""
Abstract classes for implementations of state classes

"""

import abc
import collections.abc


class StoreBase(collections.abc.Iterator):
    """
    Interface that must be implemented by all state classes

    """

    def __init__(self, _name):
        self.name = _name

    @abc.abstractmethod
    def add(self, v):
        pass

    @abc.abstractmethod
    def empty(self):
        pass

    @abc.abstractmethod
    def clear(self):
        pass

    @abc.abstractmethod
    def __iter__(self):
        pass
