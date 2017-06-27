"""Simple script to generate random price data on Kafka topic"""


from collections import namedtuple
import pandas as pd
from random_prices import RandomPrices
from source import Source

ITEM = namedtuple('ITEM', ['name', 'prob', 'seed', 'initial_price', 'sigma'])


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


def _run(sources, timestamp, freq, produce):
    stop = False
    while not stop:
        for (name, source) in sources.items():
            try:
                price = next(source)
                if price is not None:
                    produce(timestamp, name, price)
            except StopIteration:
                stop = True
        timestamp = timestamp + freq


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
    return parser


def main():
    """Main entry for script"""
    parser = _get_parser()
    args = parser.parse_args()
    sources = _get_sources(_get_items(args.items), args.limit)
    timestamp = pd.Timestamp(args.start)
    freq = pd.Timedelta(args.freq)
    if args.broker_list is None:
        def _produce(timestamp, name, price):
            print('{},{},{}'.format(timestamp, name, price))
        _run(sources, timestamp, freq, _produce)
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
                    producer.produce(args.topic, data.encode('utf-8'))
                    producer.poll(0)
                    produced = True
                except BufferError:
                    producer.poll(10)

        _run(sources, timestamp, freq, _produce)
        producer.flush()


if __name__ == '__main__':
    main()
