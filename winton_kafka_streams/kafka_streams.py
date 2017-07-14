"""
Primary entrypoint for applications wishing to implement Python Kafka Streams

"""

import logging
from enum import Enum

from .processor import StreamThread
from .kafka_client_supplier import KafkaClientSupplier

log = logging.getLogger(__name__)


class KafkaStreams:
    """
    Encapsulates stream graph processing units

    """

    """
      Kafka Streams states are the possible state that a Kafka Streams instance can be in.
      An instance must only be in one state at a time.
      Note this instance will be in "Rebalancing" state if any of its threads is rebalancing
      The expected state transition with the following defined states is:

      <pre>
                      +--------------+
              +<----- | Created      |
              |       +-----+--------+
              |             |
              |             v
              |       +-----+--------+
              +<----- | Rebalancing  | <----+
              |       +--------------+      |
              |                             |
              |                             |
              |       +--------------+      |
              +-----> | Running      | ---->+
              |       +-----+--------+
              |             |
              |             v
              |       +-----+--------+
              +-----> | Pending      |
                      | Shutdown     |
                      +-----+--------+
                            |
                            v
                      +-----+--------+
                      | Not Running  |
                      +--------------+
      </pre>
    """
    class State(Enum):
        CREATED = 0
        RUNNING = 1
        REBALANCING = 2
        PENDING_SHUTDOWN = 3
        NOT_RUNNING = 4

        def valid_transition_to(self, new_state):
            if self is self.CREATED:
                return new_state in (self.REBALANCING, self.RUNNING, self.PENDING_SHUTDOWN)
            elif self is self.RUNNING:
                return new_state in (self.REBALANCING, self.PENDING_SHUTDOWN)
            elif self is self.REBALANCING:
                return new_state in (self.RUNNING, self.REBALANCING, self.PENDING_SHUTDOWN)
            elif self is self.PENDING_SHUTDOWN:
                return new_state in (self.NOT_RUNNING,)
            else: # including NOT_RUNNING
                return False

        def is_running(self):
            return self in (self.RUNNING, self.REBALANCING)

        def is_created_or_running(self):
            return self.is_running() or self == self.CREATED

        def __str__(self):
            return self.name

    def __init__(self, topology, kafka_config):
        self.topology = topology
        self.kafka_config = kafka_config

        self.state = self.State.CREATED

        self.consumer = None

        self.stream_thread = StreamThread(topology, kafka_config, KafkaClientSupplier(self.kafka_config))

    def set_state(self, new_state):
        old_state = self.state
        if not old_state.valid_transition_to(new_state):
            log.warn(f'Unexpected state transition from {old_state} to {new_state}.')
        else:
            log.info(f'State transition from {old_state} to {new_state}.')
        self.state = new_state

    def start(self):
        log.debug('Starting Kafka Streams process')
        if self.state == self.State.CREATED:
            self.set_state(self.State.RUNNING)
            self.stream_thread.start()
        else:
            raise KafkaStreamsError('KafkaStreams already started')

    def close(self):
        if self.state.is_created_or_running():
            self.set_state(self.State.PENDING_SHUTDOWN)
            self.stream_thread.close()
            self.set_state(self.State.NOT_RUNNING)
