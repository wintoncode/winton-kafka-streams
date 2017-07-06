"""
Python Kafka Streams example script for price binning
"""

import logging
import pandas as pd
from winton_kafka_streams.processor import BaseProcessor, TopologyBuilder
import winton_kafka_streams.kafka_config as kafka_config
import winton_kafka_streams.kafka_stream as kafka_stream

LOGGER = logging.getLogger(__name__)

_VERBOSITY = {
    0: logging.WARN,
    1: logging.INFO,
    2: logging.DEBUG
}


class Binning(BaseProcessor):
    """
    Implementation of binning process

    The code will be passed a value from the 'prices' source topic
    in Kafka. This processor will search for the final value in the
    binning range (1 minute) and output that to the 'bin-prices'
    sink topic in Kafka.

    There is a Python generator script provided to generate prices
    with normall distributed returns. You can control the frequency
    of generation, the mean and standard deviationm and the numnber
    of items generated.

    TODO: Later this example should be extended to show partition
          support.
    """

    def __init__(self):
        super().__init__()
        self.store = None

    def initialise(self, _name, _context):
        super().initialise(_name, _context)
        self.store = {}  # TODO: Replace with self.context.get_store(")
        self.context.schedule(60)

    def process(self, _, value):
        """
        Accept a value from the

        Parameters:
        -----------
        _ : object, unused (key)
            The key read from the source topic (unused here)
        value: object
            The value read from the source topic

        Returns:
        --------
          None
        """
        timestamp, name, price = value.decode('utf-8').split(',')
        timestamp = pd.Timestamp(timestamp)

        bin_ts = pd.Timestamp(
            year=timestamp.year, month=timestamp.month, day=timestamp.day,
            hour=timestamp.hour, minute=timestamp.minute, second=0
        ) + pd.Timedelta('1min')
        key = '{},{}'.format(bin_ts.isoformat(), name).encode('utf-8')

        if key not in self.store and len(self.store) == 2:
            self.punctuate()  # TODO need a better way to do that
        self.store[key] = price.encode('utf-8')

        self.context.commit()

    def punctuate(self):
        """Produce output"""
        for k in sorted(self.store):
            self.context.forward(k, self.store[k])
            LOGGER.debug('Forwarding to sink  (%s, %s)', k, self.store[k])
        self.store = {}


def run(config_file = None):
    """
    Starts the binning process

    Called here from main() when invoked from command line
    but could equally import binning and call
    binning.run(config_file)

    """
    if config_file:
        kafka_config.read_local_config(config_file)

    with TopologyBuilder() as topology_builder:
        topology_builder. \
            source('prices', ['prices']). \
            processor('binner', Binning, 'prices'). \
            sink('result', 'bin-prices', 'binner')

    wks = kafka_stream.KafkaStream(topology_builder, kafka_config)
    wks.start()


def _get_parser():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--config-file', '-c', default='config.properties',
        help="Local configuration - will override internal defaults"
    )
    parser.add_argument(
        '-v', dest='verbosity', action='count', default=0,
        help='Enable more verbose logging, use once for info, '
             'twice for debug.'
    )
    return parser


def main():
    parser = _get_parser()
    args = parser.parse_args()
    logging.basicConfig(level=_VERBOSITY.get(args.verbosity, logging.DEBUG))
    run(args.config_file)


if __name__ == '__main__':
    main()
