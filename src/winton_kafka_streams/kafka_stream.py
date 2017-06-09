"""
Primary entrypoint for applications wishing to implement Python Kafka Streams

"""

import logging

import confluent_kafka as kafka

log = logging.getLogger(__name__)

class KafkaStream(object):
    """
    Encapsulates stream graph processing units

    """

    def __init__(self, topology, kafka_config):
        self.topology = topology
        self.kafka_config = kafka_config

        self.consumer = None

    def start(self):
        """
        Begin streaming the data across the topology

        """
        self.consumer = kafka.Consumer({'bootstrap.servers': self.kafka_config.BOOTSTRAP_SERVERS, 'group.id': 'test'})

        #, 'group.id': 'testgroup',
        #'default.topic.config': {'auto.offset.reset': 'smallest'}})
        log.debug('Subscribing to topics %s', self.topology.kafka_topics)
        self.consumer.subscribe(self.topology.kafka_topics)
        log.debug('Subscribed to topics')

        self.run()


    def run(self):
        running = True
        while running:
            msg = self.consumer.poll()
            if msg is None:
                continue
            elif not msg.error():
                print('Received message: %s' % msg.value().decode('utf-8'))
            elif msg.error().code() != kafka.KafkaError._PARTITION_EOF:
                print(msg.error())
                running = False
        self.consumer.close()


    
