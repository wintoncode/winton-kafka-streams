"""
Default context passed to every processor

"""

from . import _context

class ProcessorContext(_context.Context):
    def __init__(self):
        super().__init__()

    def send(self, topic, key, obj):
        """
        Send the key value-pair to a Kafka topic

        """

        print(f"Send {str(obj)} to topic '{topic}'")
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

    def forward(self, key, value):
        """
        Forward the values directly to the next node in the topology

        """
        previousNode = self.currentNode
        try:
            for child in self.currentNode.children:
                self.currentNode = child
                child.process(key, value)
        finally:
            self.currentNode = previousNode