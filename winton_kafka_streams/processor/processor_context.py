"""
Default context passed to every processor

"""
import logging

from . import _context

log = logging.getLogger(__name__)


class ProcessorContext(_context.Context):
    """
    The same processor context is shared betwen all nodes in
    a single topology instance. It takes care of forwarding
    values to downstream processors.

    """
    def __init__(self, _task_id, _task, _record_collector, _state_record_collector, _state_stores):

        super().__init__(_state_record_collector, _state_stores)

        self.application_id = _task.application_id
        self.task_id = _task_id
        self.task = _task
        self.record_collector = _record_collector

    def commit(self):
        """
        Request a commit

        Returns:
        --------
         - None

        """

        self.task.need_commit()

    def forward(self, key, value):
        """
        Forward the key/value to the next node in the topology

        """
        previous_node = self.current_node
        try:
            for child in self.current_node.children:
                self.current_node = child
                child.process(key, value)
        finally:
            self.current_node = previous_node

    def schedule(self, timestamp):
        """
        Schedule the punctuation function call

        """
        self.task.schedule(timestamp)
