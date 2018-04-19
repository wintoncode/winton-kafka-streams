"""
Kafka consumer poll thread

"""

import logging
import threading
from enum import Enum
from itertools import zip_longest

from confluent_kafka import KafkaError

from ..errors.task_migrated_error import TaskMigratedError
from .task_id import TaskId
from ._stream_task import StreamTask


class StreamThread:
    """
      Stream thread states are the possible states that a stream thread can be in.
      A thread must only be in one state at a time
      The expected state transitions with the following defined states is:

      <pre>
                     +-------------+
                     | Not Running | <-------+
                     +-----+-------+         |
                           |                 |
                           v                 |
                     +-----+-------+         |
               +<--- | Running     | <----+  |
               |     +-----+-------+      |  |
               |           |              |  |
               |           v              |  |
               |     +-----+-------+      |  |
               +<--- | Partitions  |      |  |
               |     | Revoked     |      |  |
               |     +-----+-------+      |  |
               |           |              |  |
               |           v              |  |
               |     +-----+-------+      |  |
               |     | Assigning   |      |  |
               |     | Partitions  | ---->+  |
               |     +-----+-------+         |
               |           |                 |
               |           v                 |
               |     +-----+-------+         |
               +---> | Pending     | ------->+
                     | Shutdown    |
                     +-------------+
      </pre>
    """

    class State(Enum):
        NOT_RUNNING = 0
        RUNNING = 1
        PARTITIONS_REVOKED = 2
        ASSIGNING_PARTITIONS = 3
        PENDING_SHUTDOWN = 4

        def valid_transition_to(self, new_state):
            if self is self.NOT_RUNNING:
                return new_state in (self.RUNNING,)
            elif self is self.RUNNING:
                return new_state in (self.PARTITIONS_REVOKED, self.PENDING_SHUTDOWN)
            elif self is self.PARTITIONS_REVOKED:
                return new_state in (self.PENDING_SHUTDOWN, self.ASSIGNING_PARTITIONS)
            elif self is self.ASSIGNING_PARTITIONS:
                return new_state in (self.RUNNING, self.PENDING_SHUTDOWN)
            elif self is self.PENDING_SHUTDOWN:
                return new_state in (self.NOT_RUNNING,)
            else:
                return False

        def is_running(self):
            return self not in (self.NOT_RUNNING, self.PENDING_SHUTDOWN)

        def __str__(self):
            return self.name

    def __init__(self, _topology_builder, _config, _kafka_supplier):
        super().__init__()
        self.topology_builder = _topology_builder
        self.config = _config
        self.kafka_supplier = _kafka_supplier

        self.tasks = []
        self.tasks_by_partition = {}
        self.state = self.State.NOT_RUNNING

        self.topics = _topology_builder.topics

        self.thread = threading.Thread(target=self.run)
        self.log = logging.getLogger(__name__ + '(' + self.thread.name + ')')

        self.log.info('Topics for consumer are: %s', self.topics)
        self.consumer = self.kafka_supplier.consumer()

        self.state_listener = None
        self.set_state(self.State.RUNNING)

    def thread_id(self):
        return self.thread.ident

    def set_state(self, new_state):
        old_state = self.state
        if not old_state.valid_transition_to(new_state):
            self.log.warning(f'Unexpected state transition from {old_state} to {new_state}.')
        else:
            self.log.info(f'State transition from {old_state} to {new_state}.')
        self.state = new_state
        if self.state_listener:
            self.state_listener(self, old_state, new_state)

    def set_state_when_not_in_pending_shutdown(self, new_state):
        if self.state is not self.State.PENDING_SHUTDOWN:
            self.set_state(new_state)

    def set_state_listener(self, listener):
        """ For internal use only. """
        self.state_listener = listener

    def still_running(self):
        return self.state.is_running()

    def start(self):
        self.thread.start()

    def run(self):
        self.log.debug('Running stream thread...')
        try:
            self.consumer.subscribe(self.topics, on_assign=self.on_assign, on_revoke=self.on_revoke)

            while self.still_running():
                try:
                    records = self.poll_requests(0.1)
                    if records:
                        self.log.debug(f'Processing {len(records)} record(s)')
                        self.add_records_to_tasks(records)
                        self.process_and_punctuate()
                except TaskMigratedError as error:
                    self.log.warning(f"Detected a task that got migrated to another thread. " +
                                     "This implies that this thread missed a rebalance and dropped out of the "
                                     "consumer group. " +
                                     "Trying to rejoin the consumer group now. %s", error)

            self.log.debug('Ending stream thread...')
        finally:
            self.commit_all()
            self.shutdown()

    def poll_requests(self, poll_timeout):
        """ Get the next batch of records """

        # The current python kafka client gives us messages one by one,
        # but for better throughput we want to process many records at once.
        # Keep polling until we get no more records out.
        records = []
        record = self.consumer.poll(poll_timeout)
        while record is not None:
            if not record.error():
                self.log.debug('Received message at offset: %d', record.offset())
                records.append(record)
                record = self.consumer.poll(0.)
            elif record.error().code() == KafkaError._PARTITION_EOF:
                record = self.consumer.poll(0.)
            elif record.error():
                self.log.error('Record error received: %s', record.error())

        return records

    def add_records_to_tasks(self, records):
        for record in records:
            self.tasks_by_partition[record.partition()].add_records([record])

    def process_and_punctuate(self):
        while True:
            total_processed_each_round = 0

            for task in self.tasks:
                if task.process():
                    total_processed_each_round += 1

            if total_processed_each_round == 0:
                break

        for task in self.tasks:
            task.maybe_punctuate()
            if task.commit_needed():
                self.commit(task)

    def commit(self, task):
        self.log.debug('Commit task "%s"', task)
        task.commit()

    def commit_all(self):
        for task in self.tasks:
            self.commit(task)

    def shutdown(self):
        self.set_state(self.State.NOT_RUNNING)

    def add_stream_tasks(self, assignment):
        # simplistic, but good enough for now. should take co-locating topics etc. into account in the future
        grouped_tasks = {TaskId(topic_partition.topic, topic_partition.partition): {topic_partition}
                         for topic_partition in assignment}
        self.tasks = [StreamTask(task_id, self.config.APPLICATION_ID,
                                 partitions, self.topology_builder, self.consumer,
                                 self.kafka_supplier.producer(), self.config)
                      for (task_id, partitions)
                      in grouped_tasks.items()]

        for task in self.tasks:
            self.tasks_by_partition.update(
                zip_longest((topic_partition.partition for topic_partition in task.partitions), [], fillvalue=task))

    def on_assign(self, consumer, partitions):
        self.log.debug('Assigning partitions %s', partitions)

        self.set_state_when_not_in_pending_shutdown(self.State.ASSIGNING_PARTITIONS)
        self.add_stream_tasks(partitions)
        self.set_state_when_not_in_pending_shutdown(self.State.RUNNING)

    def on_revoke(self, consumer, partitions):
        self.log.debug('Revoking partitions %s', partitions)
        self.commit_all()
        self.set_state_when_not_in_pending_shutdown(self.State.PARTITIONS_REVOKED)
        self.tasks = []
        self.tasks_by_partition = {}

    def close(self):
        self.log.debug('Closing stream thread and consumer')
        self.set_state(self.State.PENDING_SHUTDOWN)
        self.consumer.close()
