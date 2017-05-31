"""
Winton Kafka Streams

Main entrypoints

"""

import sys

from confluent_kafka import Consumer, KafkaError

from .processor import *
from . import kafka_config
from . import kafka_stream

class T(object):
    def __init__(self, *kafka_topics):
        self.kafka_topics = list(kafka_topics)


def _debug_run():
    topology = T('streams-file-input')
    ks = kafka_stream.KafkaStream(topology, kafka_config)
    ks.start()


def _debug_run_old():
    from .stream import KafkaStream

    ks = KafkaStream(T(['test']))
    ks.start()

def _debug_run_basic_kafka():
    print("Running debug demo...")

    c = Consumer({'bootstrap.servers': 'localhost', 'group.id': 'testroup',
                'default.topic.config': {'auto.offset.reset': 'smallest'}})
    c.subscribe(['words'])
    running = True
    while running:
        msg = c.poll()
        if not msg.error():
            print('Received message: %s' % msg.value().decode('utf-8'))
        elif msg.error().code() != KafkaError._PARTITION_EOF:
            print(msg.error())
            running = False
    c.close()



if __name__ == '__main__':
    import argparse

    class _StreamArgParser(argparse.Action):
        def __init__(self, option_strings, dest, nargs=None, **kwargs):
            super(option_strings, dest, nargs, **kwargs)
            if nargs is not None:
                raise ValueError("nargs not allowed")

        def __call__(self, parser, namespace, values, option_string=None):
            ck, _, v = values.partition('=')
            setattr(namespace, self.dest, (ck, v))
            if not hasattr(kafka_config, ck):
                raise ValueError("Config option '{}' is unrecognised".format(ck))
            setattr(kafka_config, ck, v)


    import logging
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(description="Debug runner for Python Kafka Streams")
    parser.add_argument('--stream-arg', '-a', help="Streams argument", )
    parser.parse_args()

    _debug_run()
