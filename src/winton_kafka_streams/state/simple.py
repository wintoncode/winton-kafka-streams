"""
Simple state implementation. 

Not intended for anything other than development and debugging. 

"""

import queue

from ._abc import StateBase

class SimpleState(StateBase):
    """
    State class that holds all transform calculations in a simple array
    """
    def __init__(self):
        self.values = queue.Queue()

    def add_result(self, value):
        """
        Add a value to the end of the values queue

        """
        self.values.put(value)

    def has_data(self):
        """
        Checks if the values queue has any data

        """
        return not self.values.empty()
