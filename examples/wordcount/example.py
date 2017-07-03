"""
Winton Kafka Streams

Main entrypoints

"""

import logging
import collections

from winton_kafka_streams.processor import BaseProcessor, TopologyBuilder
from winton_kafka_streams.state.simple import SimpleStore
import winton_kafka_streams.kafka_config as kafka_config
import winton_kafka_streams.kafka_stream as kafka_stream

log = logging.getLogger(__name__)

class WordCount(BaseProcessor):
    def __init__(self):
        super().__init__()
        self.store = None

    def initialise(self, _name, _context):
        super().initialise(_name, _context)
        self.store = collections.Counter()

    def process(self, key, value):
        self.store.update(value.decode('utf-8').split())

        # TODO: In absence of a punctuate call schedule running:
        self.punctuate()

        self.context.commit()

    def punctuate(self):
        for k, v in self.store.items():
            log.debug('Forwarding to sink  (%s, %s)', k, v)
            self.context.forward(k, str(v))


def run(config_file):
    kafka_config.read_local_config(config_file)

    # Can also directly set config variables inline in Python
    #kafka_config.KEY_SERDE = MySerde

    with TopologyBuilder() as topology_builder:
        topology_builder. \
            source('input-value', ['wks-wordcount-example-topic']). \
            processor('count', WordCount, 'input-value'). \
            sink('output-count', 'wks-wordcount-example-count', 'count')

    wks = kafka_stream.KafkaStream(topology_builder, kafka_config)
    wks.start()



if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="Debug runner for Python Kafka Streams")
    parser.add_argument('--config-file', '-c',
                        help="Local configuration - will override internal defaults",
                        default='config.properties')
    parser.add_argument('--verbose', '-v',
                        help="Increase versbosity (repeat to increase level)",
                        action='count', default=0)
    args = parser.parse_args()

    levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    logging.basicConfig(level=levels.get(args.verbose, logging.DEBUG))

    run(args.config_file)
