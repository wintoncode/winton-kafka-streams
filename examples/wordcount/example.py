"""
Winton Kafka Streams

Main entrypoints

"""

import logging

from winton_kafka_streams.processor import BaseProcessor, TopologyBuilder
from winton_kafka_streams.state.simple import SimpleStore
import winton_kafka_streams.kafka_config as kafka_config
import winton_kafka_streams.kafka_stream as kafka_stream

log = logging.getLogger(__name__)

class WordCount(BaseProcessor):
    def __init__(self):
        super().__init__()

    def initialise(self, _name, _context):
        super().initialise(_name, _context)
        self.store = self.context.get_store("word-counts")

        self.context.schedule(1000)

    def process(self, key, value):
        n_words = len(value.split())
        self.store.add(key, str(n_words))

        # TODO: In absence of a punctuate call schedule running:
        self.punctuate()

        self.context.commit()

    def punctuate(self):
        for k, v in iter(self.store):
            log.debug('Forwarding to sink  (%s, %s)', k, v)
            self.context.forward(k, v)
        self.store.clear()


def _debug_run(config_file):
    kafka_config.read_local_config(config_file)

    # Can also directly set config variables inline in Python
    #kafka_config.KEY_SERDE = MySerde

    word_count_store = SimpleStore('word-counts')

    with TopologyBuilder() as topology_builder:
        topology_builder. \
            source('input-value', ['wks-wordcount-example-topic']). \
            processor('count', WordCount, 'input-value', stores=[word_count_store]). \
            sink('output-count', 'wks-wordcount-example-count', 'count')

    wks = kafka_stream.KafkaStream(topology_builder, kafka_config)
    wks.start()



if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)

    import argparse

    parser = argparse.ArgumentParser(description="Debug runner for Python Kafka Streams")
    parser.add_argument('--config-file', '-c', help="Local configuration - will override internal defaults", default='config.properties')
    args = parser.parse_args()

    _debug_run(args.config_file)
