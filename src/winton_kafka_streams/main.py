"""
Winton Kafka Streams

Main entrypoints

"""

import sys

from confluent_kafka import Consumer, KafkaError

from .processor import start_consumer
from .processor import BaseProcessor, ProcessorContext, TopologyBuilder
from .state.simple import SimpleStore
from . import kafka_config
from . import kafka_stream

class DoubleProcessor(BaseProcessor):
    def __init__(self, _name, _context):
        super().__init__()

    def initialise(self, _name, _context):
        super().initialise(_name, _context)
        self.store = self.context.get_store("prices")

        self.context.schedule(1000)

    def process(self, key, value):
        self.store.add(key, value)

        print("DoubleProcessor::process("+str(key)+", "+str(value)+")")

        # TODO: In absence of a punctuate call schedule running:
        if len(self.store) == 4:
            self.punctuate()

        self.context.commit()

    def punctuate(self):
        for k, v in iter(self.store):
            self.context.forward(k, v)
        self.store.clear()


def _debug_run():
    double_store = SimpleStore('simple-store')
    context = ProcessorContext()
    context.add_store('prices', double_store)

    topology = TopologyBuilder()
    src = topology.source('prices', 'price')
    proc_double = topology.processor('double', DoubleProcessor('double', context), 'prices')
    result = topology.sink('result', 'priceX2', 'double')

    # TODO: This is out of place - we can have other context too
    context.currentNode = src

    # TODO: Nodes not initialised correctly
    src.initialise(context)
    proc_double.initialise(context)
    result.initialise(context)

    #topology.pprint(sys.stdout)

    kct = start_consumer(['prices'], context, src)
    kct.join()


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
