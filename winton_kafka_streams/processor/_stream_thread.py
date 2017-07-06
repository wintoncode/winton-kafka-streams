"""
Kafka consumer poll thread

"""

import queue
import logging
import itertools
import threading
from enum import Enum

from confluent_kafka import KafkaError, TopicPartition

from ._record_collector import RecordCollector
from .processor_context import ProcessorContext

log = logging.getLogger(__name__)

class StreamTask:
    def __init__(self, _task_id, _application_id, _partitions, _topology_builder, _consumer, _producer):
        self.task_id = _task_id
        self.application_id = _application_id
        self.partitions = _partitions
        self.topology = _topology_builder.build()
        self.consumer = _consumer
        self.producer = _producer

        self.recordCollector = RecordCollector(self.producer)

        self.queue = queue.Queue()
        self.context = ProcessorContext(self, self.recordCollector)

        self.needCommit = False
        self.consumedOffsets = {}

        self._init_topology(self.context)

    def _init_topology(self, context):
        for node in self.topology.nodes.values():
            try:
                context.currentNode = node
                node.initialise(context)
            finally:
                context.currentNode = None
                context.currentRecord = None

    def add_records(self, records):
        for record in records:
            self.queue.put(record)

    def process(self):
        if self.queue.empty():
            return False

        record = self.queue.get()
        self.context.currentRecord = record

        # TODO: FIXME-  assumes only one topic (next two lines)
        self.context.currentNode = self.topology.sources[0]
        self.topology.sources[0].process(record.key(), record.value())

        self.consumedOffsets[(record.topic(), record.partition())] = record.offset()

        self.context.currentRecord = None

    def commit(self):
        for ((t, p), o) in self.consumedOffsets.items():
            self.consumer.commit(offsets=[TopicPartition(t, p, o+1)], async=False)
        self.needCommit = False
        self.consumedOffsets.clear()

    def __repr__(self):
        return self.__class__.__name__ + f":{self.task_id}"


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
                return new_state in (self.PARTITIONS_REVOKED,)
            elif self is self.PARTITIONS_REVOKED:
                return new_state in (self.PENDING_SHUTDOWN, self.ASSIGNING_PARTITIONS)
            elif self is self.ASSIGNING_PARTITIONS:
                return new_state in (self.RUNNING, self.PENDING_SHUTDOWN)
            elif self is self.PENDING_SHUTDOWN:
                return new_state in (self.NOT_RUNNING,)
            else:
                return False

        def is_running(self):
            return not self in (self.NOT_RUNNING, self.PENDING_SHUTDOWN)


    def __init__(self, _topology, _config, _kafka_supplier):
        super().__init__()
        self.topology = _topology
        self.config = _config
        self.kafka_supplier = _kafka_supplier

        self.tasks = []
        self.state = self.State.NOT_RUNNING

        self.topics = _topology.topics

        log.info('Topics for consumer are: %s', self.topics)
        self.consumer = self.kafka_supplier.consumer()

        self.thread = threading.Thread(target=self.run)
        self.set_state(self.State.RUNNING)

    def set_state(self, new_state):
        old_state = self.state
        if not old_state.valid_transition_to(new_state):
            log.warn(f'Unexpected state transition from {old_state} to {new_state}.')
        else:
            log.info(f'State transition from {old_state} to {new_state}.')
        self.state = new_state

    def set_state_when_not_in_pending_shutdown(self, new_state):
        if not self.state is self.State.PENDING_SHUTDOWN:
            self.set_state(new_state)

    def still_running(self):
        return self.state.is_running()

    def start(self):
        self.thread.start()

    def run(self):
        log.debug('Running stream thread...')
        try:
            self.consumer.subscribe(self.topics, on_assign=self.on_assign, on_revoke=self.on_revoke)

            while self.still_running():
                record = self.consumer.poll()
                if not record.error():
                    log.debug('Received message: %s', record.value().decode('utf-8'))
                    self.processAndPunctuate(record)
                elif record.error().code() == KafkaError._PARTITION_EOF:
                    continue
                elif record.error():
                    log.error('Record error received: %s', record.error())

            log.debug('Ending stream thread...')
        finally:
            # TODO: @ah- must commit offsets
            self.shutdown()

    def processAndPunctuate(self, record):
        task = self.tasks[record.partition()]
        task.add_records([record])
        task.process()
        if task.needCommit:
            self.commit(task)

    def commit(self, task):
        try:
            log.debug('Commit task "%s"', task)
            task.commit()
        except CommitFailedException as cfe:
            log.warn('Failed to commit')
            log.exception(cfe)
            pass
        except KafkaException as ke:
            log.exception(ke)
            raise

    def shutdown(self):
        self.set_state(self.State.NOT_RUNNING)


    def on_assign(self, consumer, partitions):
        log.debug('Assigning partitions %s', partitions)

        self.set_state_when_not_in_pending_shutdown(self.State.ASSIGNING_PARTITIONS)
        # TODO: task_id == 0 is not correct, fix
        self.tasks = [StreamTask(0, self.config.APPLICATION_ID,
                                 partitions, self.topology, consumer,
                                 self.kafka_supplier.producer())]

        self.set_state_when_not_in_pending_shutdown(self.State.RUNNING)

    def on_revoke(self, consumer, partitions):
        # TODO: @ah- need to commit offsets during rebalance
        log.debug('Revoking partitions %s', partitions)
        self.set_state_when_not_in_pending_shutdown(self.State.PARTITIONS_REVOKED)
        self.tasks = []

    def close(self):
        log.debug('Closing stream thread and consumer')
        self.set_state(self.State.PENDING_SHUTDOWN)
        self.consumer.close()
