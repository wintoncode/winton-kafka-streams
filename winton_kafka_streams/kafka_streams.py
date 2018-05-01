"""
Primary entrypoint for applications wishing to implement Python Kafka Streams

"""

import logging
import threading
from enum import Enum

from .errors.kafka_streams_error import KafkaStreamsError
from .kafka_client_supplier import KafkaClientSupplier
from .processor import StreamThread

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
            else:  # including NOT_RUNNING
                return False

        def is_running(self):
            return self in (self.RUNNING, self.REBALANCING)

        def is_created_or_running(self):
            return self.is_running() or self == self.CREATED

        def __str__(self):
            return self.name

    def __init__(self, topology_builder, kafka_config):
        self.kafka_config = kafka_config

        self.state = self.State.CREATED
        self.state_lock = threading.Lock()
        self.thread_states = {}

        self.consumer = None

        self.stream_threads = [StreamThread(topology_builder, self.kafka_config, KafkaClientSupplier(self.kafka_config))
                               for _ in range(int(self.kafka_config.NUM_STREAM_THREADS))]
        for stream_thread in self.stream_threads:
            stream_thread.set_state_listener(self.on_thread_state_change)
            self.thread_states[stream_thread.thread_id()] = stream_thread.state

    def set_state(self, new_state):
        old_state = self.state
        if not old_state.valid_transition_to(new_state):
            log.warn(f'Unexpected state transition from {old_state} to {new_state}.')
        else:
            log.info(f'State transition from {old_state} to {new_state}.')
        self.state = new_state

    def on_thread_state_change(self, stream_thread, old_state, new_state):
        with self.state_lock:
            self.thread_states[stream_thread.thread_id()] = new_state
            if new_state in (StreamThread.State.ASSIGNING_PARTITIONS, StreamThread.State.PARTITIONS_REVOKED):
                self.set_state(self.State.REBALANCING)
            elif set(self.thread_states.values()) == set([StreamThread.State.RUNNING]):
                    self.set_state(self.State.RUNNING)

    def start(self):
        log.debug('Starting Kafka Streams process')
        if self.state == self.State.CREATED:
            self.set_state(self.State.RUNNING)
            for stream_thread in self.stream_threads:
                stream_thread.start()
        else:
            raise KafkaStreamsError('KafkaStreams already started.')

    def close(self):
        if self.state.is_created_or_running():
            self.set_state(self.State.PENDING_SHUTDOWN)
            for stream_thread in self.stream_threads:
                stream_thread.set_state_listener(None)
                stream_thread.close()
            self.set_state(self.State.NOT_RUNNING)
