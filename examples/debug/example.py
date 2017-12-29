"""
Winton Kafka Streams

Main entrypoints

"""

import logging
import time

from winton_kafka_streams.processor import BaseProcessor, TopologyBuilder
from winton_kafka_streams.state.simple import SimpleStore
import winton_kafka_streams.kafka_config as kafka_config
import winton_kafka_streams.kafka_streams as kafka_streams

log = logging.getLogger(__name__)

class DoubleProcessor(BaseProcessor):
    """
    Example processor that will double the value passed in

    """

    def process(self, key, value):
        log.debug(f'DoubleProcessor::process({key}, {str(value)})')
        doubled = value*2
        log.debug(f'Forwarding to sink ({key}, {str(doubled)})')
        self.context.forward(key, doubled)

    # TODO -- finish off the spec from the README, need to keep state


def _debug_run(config_file):
    kafka_config.read_local_config(config_file)

    with TopologyBuilder() as topology_builder:
        topology_builder. \
            source('input-value', ['wks-debug-example-topic-two']). \
            processor('double', DoubleProcessor, 'input-value'). \
            sink('output-double', 'wks-debug-example-output', 'double'). \
            state_store('double-store', SimpleStore, 'double')

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

    logging.basicConfig(level=logging.DEBUG)

    import argparse

    parser = argparse.ArgumentParser(description="Debug runner for Python Kafka Streams")
    parser.add_argument('--config-file', '-c', help="Local configuration - will override internal defaults",
                        default='config.properties')
    args = parser.parse_args()

    _debug_run(args.config_file)
