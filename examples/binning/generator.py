"""
Simple script to generate prices values with normally
distributed returns on a Kafka 'prices' topic.

Run ./generator --help to see the full range of options.

"""

import logging
from collections import namedtuple
import datetime as dt
import time
import pandas as pd
from random_prices import RandomPrices
from source import Source

ITEM = namedtuple('ITEM', ['name', 'prob', 'seed', 'initial_price', 'sigma'])

LOGGER = logging.getLogger(__name__)

_VERBOSITY = {
    0: logging.WARN,
    1: logging.INFO,
    2: logging.DEBUG
}


def _get_items(items):
    parsed_items = []
    for item in items:
        vals = item.split(',')
        try:
            parsed_item = ITEM(
                vals[0], float(vals[1]),
                int(vals[2]), float(vals[3]), float(vals[4])
            )
            parsed_items.append(parsed_item)
        except Exception:
            raise ValueError(
                '{} should contain 5 comma separated options: '
                'name[string],prob[float],seed[int],'
                'initial_price[float],sigma[float]'
            )
    return parsed_items


def _get_sources(items, limit):
    return {
        item.name: Source(
            item.prob,
            RandomPrices(
                item.seed, item.initial_price, item.sigma, limit
            ),
            item.seed
        )
        for item in items
    }


def _run(sources, timestamp, freq, real_time, rt_multiplier, produce):
    """
    Start the generation of prices on the 'prices' topic
    """

    stop = False
    while not stop:
        if real_time:
            start_time = dt.datetime.utcnow()
        for (name, source) in sources.items():
            try:
                price = next(source)
                if price is not None:
                    produce(timestamp, name, price)
                    LOGGER.info('%s,%s,%s', timestamp, name, price)
            except StopIteration:
                stop = True
        timestamp = timestamp + freq
        if real_time:
            duration = dt.datetime.utcnow() - start_time
            sleep_seconds = (freq.total_seconds() - duration.total_seconds()) / rt_multiplier
            if sleep_seconds < 0.0:
                LOGGER.warning(
                    'Not keeping up, lagging by %ss', -sleep_seconds
                )
            else:
                LOGGER.debug('Sleeping for %ss', sleep_seconds)
                time.sleep(sleep_seconds)


def _get_parser():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '-i', '--item', required=True, action='append', dest='items',
        help='Comma separated list of construction details for random price '
             'sources, should be name[string],prob[float],seed[int],'
             'initial_price[float],sigma[float]'
    )
    parser.add_argument(
        '-l', '--limit', dest='limit', type=int, default=1_000_000,
        help='Limit of iterations to be performed (default 1M)'
    )
    parser.add_argument(
        '-s', '--start', dest='start', default='2017-01-01',
        help='Date(time) to start the price series from, e.g. '
             '2000-01-01T10:30:12; must be a valid pandas timestamp. '
             '(default 2017-01-01)'
    )
    parser.add_argument(
        '-f', '--freq', dest='freq', default='250ms',
        help='The frequence by which to increment the time, must be a '
             'valid pandas timedelta, e.g. 30s. (default 250ms)'
    )
    parser.add_argument(
        '-kb', '--broker-list', dest='broker_list', default=None,
        help='Kafka broker list, e.g. kafka-1:9092,kafka-2:9092; also '
             'requires --topic to be specified.  If not provided output '
             'will be produced to stdout instead of Kafka.'
    )
    parser.add_argument(
        '-kt', '--topic', dest='topic', default=None,
        help='The Kafka topic to produce to, this will be ignored '
             'if --broker-list is not specified as well.'
    )
    parser.add_argument(
        '-rt', '--real-time', dest='real_time', action='store_true',
        help='Toggle (approximate) real-time generation of random '
             'prices.  This will output prices in real-time trying '
             'to match the frequency specified in --freq.'
    )
    parser.add_argument(
        '-rtm', '--real-time-multiplier', type=float, default=1.0,
        help='Speed up real time producer of prices by a factor. '
             'Default=1.0 (actual time).'
    )
    parser.add_argument(
        '-v', dest='verbosity', action='count', default=0,
        help='Enable more verbose logging (can be specified multiple '
             'times to increase verbosity)'
    )
    return parser


def main():
    """Main entry for script"""
    parser = _get_parser()
    args = parser.parse_args()
    sources = _get_sources(_get_items(args.items), args.limit)
    timestamp = pd.Timestamp(args.start)
    freq = pd.Timedelta(args.freq)
    logging.basicConfig(level=_VERBOSITY.get(args.verbosity, logging.DEBUG))
    if args.broker_list is None:
        def _produce(timestamp, name, price):
            print('{},{},{}'.format(timestamp, name, price))

        LOGGER.debug('Running in console mode')
        _run(sources, timestamp, freq, args.real_time, args.real_time_multiplier, _produce)
    else:
        if args.topic is None:
            raise ValueError('Must specify --topic when using Kafka')
        from confluent_kafka import Producer
        producer = Producer({'bootstrap.servers': args.broker_list})

        def _produce(timestamp, name, price):
            data = '{},{},{}'.format(timestamp, name, price)
            produced = False
            while not produced:
                try:
                    producer.produce(args.topic, value=data.encode('utf-8'), key=name)
                    producer.poll(0)
                    produced = True
                except BufferError:
                    producer.poll(10)

        LOGGER.debug('Producing to %s on %s', args.topic, args.broker_list)
        _run(sources, timestamp, freq, args.real_time, args.real_time_multiplier, _produce)
        producer.flush()


if __name__ == '__main__':
    main()
