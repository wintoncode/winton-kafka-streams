"""
Default context passed to every processor

"""
import logging

from . import _context

log = logging.getLogger(__name__)

class ProcessorContext(_context.Context):
    def __init__(self, _task, _recordCollector):
        super().__init__()

        self.task = _task
        self.recordCollector = _recordCollector

    def commit(self):
        """
        Request a commit

        Returns:
        --------
         - None

        """

        self.task.needCommit = True

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
