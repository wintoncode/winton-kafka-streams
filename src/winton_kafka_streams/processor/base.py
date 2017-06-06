"""
Base definitions for all processors

"""

import default_context

class Processor(object):
    """
    Abstract class to be implemented by all processing units
    """

    def __init__(self, name, transform, state=None):
        self.transform = transform
        self.downstream = []

        self.context = default_context.DefaultContext()

    def process(self, **inputs):
        v = self.transform(**inputs)
        self.context.state.add_result(v)

    def punctuate(self):
        if self.context.state.has_data():
            for data in self.context.state: # need to implement iterable protocol on state
                for next_node in self.downstream:
                    next_node.process(self, data)

class SourceProcessor(Processor):
    """
    Setup for a source processor

    """

    def __init__(self, *topics):
        super()

    def process(self, **inputs):
        self.context.state.add_result(**inputs)
        self.punctuate()

class SinkProcessor(Processor):
    """

    """

    def __init__(self, topic):
        super()

        self.topic = topic

    def process(self, **inputs):
        self.punctuate()

    def punctuate(self):
        if self.context.state.has_data():

