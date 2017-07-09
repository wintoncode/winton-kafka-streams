"""
Winton Kafka Streams

Main entrypoints

"""

import logging
import time

from winton_kafka_streams.processor import BaseProcessor, TopologyBuilder
from winton_kafka_streams.state.simple import SimpleStore
import winton_kafka_streams.kafka_config as kafka_config
import winton_kafka_streams.kafka_stream as kafka_stream

log = logging.getLogger(__name__)

class DoubleProcessor(BaseProcessor):
    """
    Example processor that will double the value passed in

    """
    def __init__(self):
        super().__init__()

    def initialise(self, _name, _context):
        super().initialise(_name, _context)
        self.store = self.context.get_store("double-store")

    def process(self, key, value):
        log.debug(f'DoubleProcessor::process({key}, {value})')
        try:
            # For the puropose of example just log and continue
            # on non-float values
            doubled = float(value)*2
        except ValueError as ve:
            log.exception(ve)
            return
        self.store.add(key, str(doubled))

        # TODO: In absence of a punctuate call schedule running:
        if len(self.store) == 4:
            self.punctuate()

            self.context.commit()

    def punctuate(self):
        log.debug('DoubleProcessor::punctuate')
        for k, v in iter(self.store):
            log.debug('Forwarding to sink  (%s, %s)', k, v)
            self.context.forward(k, v)
        self.store.clear()


def _debug_run(config_file):
    kafka_config.read_local_config(config_file)

    # Can also directly set config variables inline in Python
    #kafka_config.KEY_SERDE = MySerde

    with TopologyBuilder() as topology_builder:
        topology_builder. \
            source('input-value', ['wks-debug-example-topic-two']). \
            processor('double', DoubleProcessor, 'input-value'). \
            sink('output-double', 'wks-debug-example-output', 'double'). \
            state_store('double-store', SimpleStore, 'double')

    wks = kafka_stream.KafkaStream(topology_builder, kafka_config)
    wks.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        wks.close()


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)

    import argparse

    parser = argparse.ArgumentParser(description="Debug runner for Python Kafka Streams")
    parser.add_argument('--config-file', '-c', help="Local configuration - will override internal defaults", default='config.properties')
    args = parser.parse_args()

    _debug_run(args.config_file)
