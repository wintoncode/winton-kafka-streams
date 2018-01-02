"""
Python Kafka Streams example script for price binning
"""

import logging
import time
import pandas as pd
from winton_kafka_streams.processor import BaseProcessor, TopologyBuilder
import winton_kafka_streams.kafka_config as kafka_config
import winton_kafka_streams.kafka_streams as kafka_streams

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
    with normally distributed returns. You can control the frequency
    of generation, the mean and standard deviation and the number
    of items generated.

    TODO: Later this example should be extended to show partition
          support.
    """

    def initialise(self, _name, _context):
        super().initialise(_name, _context)
        # bins tracks last time bin and price per symbol
        self.bins = {}  # TODO: Replace with self.context.get_store(")

    def process(self, _, value):
        """
        Processes values from the source in search of the last
        value in that bin.

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
        timestamp, symbol, price = value.split(',')
        timestamp = pd.Timestamp(timestamp)

        bin_ts = pd.Timestamp(
            year=timestamp.year, month=timestamp.month, day=timestamp.day,
            hour=timestamp.hour, minute=timestamp.minute, second=0
        ) + pd.Timedelta('1min')
        bin_ts_and_price = '{},{}'.format(bin_ts.isoformat(), price)

        last_bin = self.bins.get(symbol)

        if last_bin is not None:
            last_bin_ts, last_price = last_bin.split(',')
            if last_bin_ts != bin_ts.isoformat():
                key = '{},{}'.format(last_bin_ts, symbol)
                LOGGER.debug('Forwarding to sink  (%s, %s)', key, last_price)
                self.context.forward(key, last_price)
                self.context.commit()  # TODO: implement auto-commit, remove this

        self.bins[symbol] = bin_ts_and_price


def run(config_file=None):
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

    wks = kafka_streams.KafkaStreams(topology_builder, kafka_config)
    wks.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        wks.close()


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
