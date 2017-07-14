"""
Primary entrypoint for applications wishing to implement Python Kafka Streams

"""

import logging

from .processor import StreamThread
from .kafka_client_supplier import KafkaClientSupplier

log = logging.getLogger(__name__)


class KafkaStreams:
    """
    Encapsulates stream graph processing units

    """

    def __init__(self, topology, kafka_config):
        self.topology = topology
        self.kafka_config = kafka_config

        self.consumer = None

        self.stream_thread = StreamThread(topology, kafka_config, KafkaClientSupplier(self.kafka_config))

    def start(self):
        self.stream_thread.start()

    def close(self):
        self.stream_thread.close()
