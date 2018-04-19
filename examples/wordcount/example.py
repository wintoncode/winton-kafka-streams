"""
Winton Kafka Streams

Main entrypoints

"""

import logging
import sys
import time

import winton_kafka_streams.kafka_config as kafka_config
import winton_kafka_streams.kafka_streams as kafka_streams
from winton_kafka_streams.processor import BaseProcessor, TopologyBuilder
import winton_kafka_streams.state as state_stores

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
            count = self.word_count_store.get(word, 0)
            self.word_count_store[word] = count + 1
        self.dirty_words |= set(words)

    def punctuate(self, timestamp):
        for word in self.dirty_words:
            count = str(self.word_count_store[word])
            log.debug(f'Forwarding to sink ({word}, {count})')
            self.context.forward(word, count)
        self.dirty_words = set()


def run(config_file, binary_output):
    kafka_config.read_local_config(config_file)
    if binary_output:
        kafka_config.VALUE_SERDE = 'examples.wordcount.custom_serde.StringIntSerde'

    count_store = state_stores.create('counts'). \
        with_string_keys(). \
        with_integer_values(). \
        in_memory(). \
        build()

    with TopologyBuilder() as topology_builder:
        topology_builder. \
            source('input-value', ['wks-wordcount-example-topic']). \
            processor('count', WordCount, 'input-value'). \
            state_store(count_store, 'count'). \
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
    parser.add_argument('--binary-output',
                        help="Output topic will contain 4-byte integers",
                        action='store_true')
    parser.add_argument('--verbose', '-v',
                        help="Increase versbosity (repeat to increase level)",
                        action='count', default=0)
    args = parser.parse_args()

    levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    level = levels.get(args.verbose, logging.DEBUG)
    logging.basicConfig(stream=sys.stdout, level=level)
    run(args.config_file, binary_output=args.binary_output)
