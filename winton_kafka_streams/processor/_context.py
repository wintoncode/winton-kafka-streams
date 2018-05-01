"""
Processor context is the link to kafka from individual processor objects

"""

import functools
import logging
from typing import Any, Callable

from winton_kafka_streams.state.key_value_state_store import KeyValueStateStore
from ..errors.kafka_streams_error import KafkaStreamsError

log = logging.getLogger(__name__)


def _raise_if_null_record(fn: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(fn)
    def _inner(*args, **kwargs):
        if args[0].current_record is None:
            raise KafkaStreamsError(f"Record cannot be unset when retrieving {fn.__name__}")
        return fn(*args, **kwargs)
    return _inner


class Context:
    """
    Processor context object

    """

    def __init__(self, _state_record_collector, _state_stores):
        self.current_node = None
        self.current_record = None
        self.state_record_collector = _state_record_collector
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

    @property  # type: ignore # waiting on https://github.com/python/mypy/issues/1362
    @_raise_if_null_record
    def offset(self):
        return self.current_record.offset()

    @property  # type: ignore
    @_raise_if_null_record
    def partition(self):
        return self.current_record.partition()

    @property  # type: ignore
    @_raise_if_null_record
    def timestamp(self):
        return self.current_record.timestamp()

    @property  # type: ignore
    @_raise_if_null_record
    def topic(self):
        return self.current_record.topic()

    def get_store(self, name) -> KeyValueStateStore:
        if not self.current_node:
            raise KafkaStreamsError("Access of state from unknown node")

        # TODO: Need to check for a global state here
        #       This is the reason that processors access store through context

        if name not in self.current_node.state_stores:
            raise KafkaStreamsError(f"Processor {self.current_node.name} does not have access to store {name}")
        if name not in self._state_stores:
            raise KafkaStreamsError(f"Store {name} is not found")

        return self._state_stores[name].get_key_value_store()
