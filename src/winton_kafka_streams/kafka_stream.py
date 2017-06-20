"""
Primary entrypoint for applications wishing to implement Python Kafka Streams

"""

import logging

import confluent_kafka as kafka

from .processor import StreamThread

log = logging.getLogger(__name__)

class KafkaStream:
    """
    Encapsulates stream graph processing units

    """

    def __init__(self, topology, kafka_config):
        self.topology = topology
        self.kafka_config = kafka_config

        self.consumer = None

        self.stream_thread = StreamThread(topology, kafka_config)

    def start(self):
        self.stream_thread.start()
