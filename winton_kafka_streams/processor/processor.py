"""
Base definitions for all processors

"""

import logging

log = logging.getLogger(__name__)

class BaseProcessor:
    def __init__(self):
        super().__init__()

        self.name = None
        self.context = None

    def initialise(self, _name, _context):
        self.name = _name
        self.context = _context

class SourceProcessor(BaseProcessor):
    """
    Fetches values from a kafka topic(s)and forwards
    them to child node for processing

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
    Forward values from processor nodes to the record collector
    from where they will be written to a Kafka topic

    """

    def __init__(self, _topic):
        super().__init__()
        self.topic = _topic

    def process(self, key, value):
        self._send_to_partition(key, value, self.context.timestamp)

    def punctuate(self):
        pass

    def _send_to_partition(self, key, value, timestamp):
        self.context.recordCollector.send_to_partition(self.topic, key, value, timestamp)
