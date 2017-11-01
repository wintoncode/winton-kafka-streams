"""
Processor context is the link to kafka from individual processor objects

"""

import logging
import functools

from ..kafka_streams_error import KafkaStreamsError

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

    def __init__(self, _state_stores):
        self.currentNode = None
        self.currentRecord = None
        self._state_stores = _state_stores

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

        # TODO: Need to check for a global state here
        #       This is the reason that processors access store through context

        if name not in self.currentNode.state_stores:
            raise KafkaStreamsError(f"Processor {self.currentNode.name} does not have access to store {name}")
        if name not in self._state_stores:
            raise KafkaStreamsError(f"Store {name} is not found")

        return self._state_stores[name]
