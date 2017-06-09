"""
Processor context is the link to kafka from individual processor objects

"""

from .._error import KafkaStreamsError

class Context(object):
    """
    Processor context object

    """

    def __init__(self):
        self.currentNode = None

        # TODO: State must be handled by another class
        self._stores = {}

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
        self._stores[name] = store

    def get_store(self, name):
        if not self.currentNode:
            raise KafkaStreamsError("Access of state from unknown node")

        # TODO: Need to check for a global state here

        # TODO: Should check that currentNode has access to this state
        return self._stores[name]
