"""
Kafka consumer poll thread

TODO: Definitely very rough and ready!

"""

import queue
import logging
import itertools
import threading

from confluent_kafka import Consumer, KafkaError

log = logging.getLogger(__name__)

class StreamTask:
    def __init__(self, task_id, application_id, partitions, topology, consumer):
        self.task_id = task_id
        self.application_id = application_id
        self.partitions = partitions
        self.topology = topology
        self.consumer = consumer
        self.queue = queue.Queue()

    def add_records(self, partition, records):
        for record in records:
            self.queue.put(record)

    def process(self):
        if self.queue.empty():
            return False

        record = self.queue.get()

        self.topology.sources[0].process(record.key(), record.value())

class StreamThread:
    def __init__(self, _topology, _config):
        super().__init__()
        self.topology = _topology
        self.config = _config

        self._running = True

        self.topics = list(itertools.chain.from_iterable([src.processor.topic for src in self.topology.sources]))

        log.info('Topics for consumer are: %s', self.topics)
        # TODO: read values from config
        self.consumer = Consumer({'bootstrap.servers': 'localhost', 'group.id': 'testgroup',
                    'default.topic.config': {'auto.offset.reset': 'smallest'}})
        self.consumer.subscribe(self.topics)

        self.thread = threading.Thread(target=self.run)#, daemon=True)

    def start(self):
        self.thread.start()

    def run(self):
        log.debug('Running stream thread...')

        self.consumer.subscribe(self.topics, on_assign=self.on_assign, on_revoke=self.on_revoke)

        while self._running:
            record = self.consumer.poll()
            if not record.error():
                log.debug('Received message: %s', record.value().decode('utf-8'))
                self.tasks[0].add_records(None, [record])
                while self.tasks[0].process():
                    pass
            elif record.error().code() == KafkaError._PARTITION_EOF:
                log.debug('Partition EOF')
                continue
            elif record.error():
                log.error('Record error received: %s', record.error())

        log.debug('Ending stream thread...')

    def on_assign(self, consumer, partitions):
        log.debug('Assigning partitions %s', partitions)
        # TODO: task_id == 0 is not correct, fix
        self.tasks = [StreamTask(0, self.config.APPLICATION_ID, partitions, self.topology, consumer)]

    def on_revoke(self, consumer, partitions):
        log.debug('Revoking partitions %s', partitions)
        self.tasks = []

    def close(self):
        log.debug('Closing stream thread and consumer')
        self.running = False
        self.consumer.close()
