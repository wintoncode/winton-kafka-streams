"""
Processor context is the link to kafka from individual processor objects

"""

import logging
import functools

from .._error import KafkaStreamsError

log = logging.getLogger(__name__)

def _raiseIfNullRecord(fn):
    @functools.wraps(fn)
    def _inner(*args, **kwargs):
        if args[0].currentRecord is None:
            raise KafkaStreamsError(f"Record cannot be unset when retrieving {fn.__name__}")
        return fn(*args, **kwargs)
    return _inner
class Context:
    """
    Processor context object

    """

    def __init__(self):
        self.currentNode = None
        self.currentRecord = None

    def send(self, topic, key, obj):
        """
        Send the key value-pair to a Kafka topic

        """
        print(f"Send {obj} to {topic}")
        pass

    def schedule(self, timestamp):
        """
        Schedule the punctuation function call

        """

        pass

    def commit(self):
        """
        Commit the current state, along with the upstream offset and the downstream sent data

        """

        pass

    @property
    @_raiseIfNullRecord
    def offset(self):
        return self.currentRecord.offset()

    @property
    @_raiseIfNullRecord
    def partition(self):
        return self.currentRecord.partition()

    @property
    @_raiseIfNullRecord
    def timestamp(self):
        return self.currentRecord.timestamp()

    @property
    @_raiseIfNullRecord
    def topic(self):
        return self.currentRecord.topic()

    def get_store(self, name):
        if not self.currentNode:
            raise KafkaStreamsError("Access of state from unknown node")

        log.info(f"Searching for store {name} in processor node {self.currentNode.name}")
        if not name in self.currentNode.stores:
            raise KafkaStreamsError(f"Store {name} is not found in node {self.currentNode.name}")

        # TODO: Need to check for a global state here
        #       This is the reason that processors access store through context

        return self.currentNode.stores[name]
