"""
Primary entrypoint for applications wishing to implement Python Kafka Streams

"""

import pprint
import logging

import confluent_kafka as kafka

from .processor import StreamThread

log = logging.getLogger(__name__)


class KafkaStream:
    """
    Encapsulates stream graph processing units

    """

    class KafkaSupplier:
        def __init__(self, _config):
            self.config = _config

        def consumer(self):
            log.debug('Starting consumer...')
            # TODO: Must set all config values applicable to a consumer
            consumer_args = {'bootstrap.servers': self.config.BOOTSTRAP_SERVERS,
                                   'group.id': 'testgroup',
                                   'default.topic.config': {'auto.offset.reset':
                                                            self.config.AUTO_OFFSET_RESET},
                                   'enable.auto.commit': self.config.ENABLE_AUTO_COMMIT}

            log.debug('Consumer Arguments: %s', pprint.PrettyPrinter().pformat(consumer_args))

            return kafka.Consumer(consumer_args)

        def producer(self):
            # TODO: Must set all config values applicable to a producer
            return kafka.Producer({'bootstrap.servers': self.config.BOOTSTRAP_SERVERS})


    def __init__(self, topology, kafka_config):
        self.topology = topology
        self.kafka_config = kafka_config

        self.consumer = None

        self.stream_thread = StreamThread(topology, kafka_config, self.KafkaSupplier(self.kafka_config))

    def start(self):
        self.stream_thread.start()
