"""
Processor context is the link to kafka from individual processor objects

"""

from .. import state

class Context(object):
    """
    Processor context object

    """

    def __init__(self, StateType=state.simple.SimpleState):
        self.state = StateType()

    def send(self, topic, key, obj):
        """
        Send the key value-pair to a Kafka topic

        """

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
