"""
Base definitions for all processors

"""

from .. import state


class Processor(object):
    """
    Abstract class to be implemented by all processing units
    """

    def __init__(self, name, transform, state=None):
        self.transform = transform
        self.downstream = []

        self.state = state if state else state.simple.SimpleState

    def process(self, **inputs):
        v = self.transform(**inputs)
        self.state.add_result(v)

    def punctuate(self):
        if self.state.has_data():
            for data in self.state: # need to implement iterable protocol on state
                for next_node in self.downstream:
                    next_node.process(self, data)

class SourceProcessor(Processor):
    """
    Setup for a source processor

    """

    def __init__(self, *topics):
        super()

    def process(self, **inputs):
        self.state.add_result(**inputs)
        self.punctuate()

class SinkProcessor(Processor):
    """

    """

    def __init__(self, topic):
        super()

        self.topic = topic

    def process(self, **inputs):
        self.punctuate()
