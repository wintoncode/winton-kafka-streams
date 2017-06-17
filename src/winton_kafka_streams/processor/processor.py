"""
Base definitions for all processors

"""

import logging

log = logging.getLogger(__name__)

class BaseProcessor(object):
    def __init__(self):
        super().__init__()

        self.name = None
        self.context = None

    def initialise(self, _name, _context):
        self.name = _name
        self.context = _context

class SourceProcessor(BaseProcessor):
    """
    Fetches values from kafka and forwards to child nodes for processing

    """

    def __init__(self, *args):
        super().__init__()
        self.topic = args

    def process(self, key, value):
        self.context.forward(key, value)

    def punctuate(self):
        pass

class SinkProcessor(BaseProcessor):
    """
    Values will be pushed to kafka topic

    """

    def __init__(self, _topic):
        super().__init__()

        self.topic = _topic

    def process(self, key, value):
        self.context.send(self.topic, key, value)

    def punctuate(self):
        pass

