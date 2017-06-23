"""
Processor context is the link to kafka from individual processor objects

"""

import logging

from .._error import KafkaStreamsError

log = logging.getLogger(__name__)

class Context:
    """
    Processor context object

    """

    def __init__(self):
        self.currentNode = None

        self._stores = {}
        self.currentNode = None

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

    def add_store(self, name, store):
        if name in self._stores:
            raise KafkaStreamsError(f"Store with name {name} already exists")
        log.info(f"Adding store {name} to node {self.currentNode.name}")
        self._stores[name] = store

    def get_store(self, name):
        if not self.currentNode:
            raise KafkaStreamsError("Access of state from unknown node")

        log.info(f"Searching for store {name} in processor node {self.currentNode.name}")
        if not name in self.currentNode.stores:
            raise KafkaStreamsError(f"Store {name} is not found in node {self.currentNode.name}")

        # TODO: Need to check for a global state here

        return self.currentNode.stores[name]
