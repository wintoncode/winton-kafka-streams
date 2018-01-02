"""
Winton Kafka Streams

Main entrypoints

"""

import logging
import time
import sys
import collections

from winton_kafka_streams.processor import BaseProcessor, TopologyBuilder
from winton_kafka_streams.state import InMemoryKeyValueStore, ChangeLoggingKeyValueStore
import winton_kafka_streams.kafka_config as kafka_config
import winton_kafka_streams.kafka_streams as kafka_streams

log = logging.getLogger(__name__)

# An example implementation of word count,
# showing where punctuate can be useful
class WordCount(BaseProcessor):

    def initialise(self, _name, _context):
        super().initialise(_name, _context)
        self.word_count_store = _context.get_store('counts')
        # dirty_words tracks what words have changed since the last punctuate
        self.dirty_words = set()
        # output updated counts every 10 seconds
        self.context.schedule(10.)

    def process(self, key, value):
        words = value.split()
        log.debug(f'words list ({words})')
        for word in words:
            count = self.word_count_store.get(word, '0')
            self.word_count_store[word] = str(int(count) + 1)
        self.dirty_words |= set(words)

    def punctuate(self, timestamp):
        for word in self.dirty_words:
            count = str(self.word_count_store[word])
            log.debug(f'Forwarding to sink ({word}, {count})')
            self.context.forward(word, count)
        self.dirty_words = set()


def run(config_file):
    kafka_config.read_local_config(config_file)

    count_store = lambda name: ChangeLoggingKeyValueStore(name, InMemoryKeyValueStore)
    with TopologyBuilder() as topology_builder:
        topology_builder. \
            source('input-value', ['wks-wordcount-example-topic']). \
            processor('count', WordCount, 'input-value'). \
            state_store('counts', count_store, 'count'). \
            sink('output-count', 'wks-wordcount-example-count', 'count')

    wks = kafka_streams.KafkaStreams(topology_builder, kafka_config)
    wks.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        wks.close()


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
    level = levels.get(args.verbose, logging.DEBUG)
    logging.basicConfig(stream=sys.stdout, level=level)
    run(args.config_file)
