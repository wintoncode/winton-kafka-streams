"""
Simple state implementation. 

Not intended for anything other than development and debugging. 

"""

import queue

from ._abc import StoreBase

class SimpleStore(StoreBase):
    """
    State class that holds all transform calculations in a simple array
    """
    def __init__(self):
        self.values = queue.Queue()

    def add(self, key, value):
        """
        Add a value to the end of the values queue

        """
        self.values.put((key, value))

    def empty(self):
        """
        Checks if the values queue has any data

        """
        return self.values.empty()

    def clear(self):
        self.values = queue.Queue()

    def __iter__(self):
        class _IterSimpleStore(object):
            def __init__(self, _simple_store):
                self.store = _simple_store
            def __iter__(self):
                return self
            def __next__(self):
                if self.store.values.empty():
                    raise StopIteration
                return self.store.values.get()

        return _IterSimpleStore(self)

    def __len__(self):
        return self.values.qsize()

    def __next__(self):
        pass