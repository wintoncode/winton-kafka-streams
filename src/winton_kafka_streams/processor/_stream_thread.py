"""
Kafka consumer poll thread

"""

import queue
import logging
import itertools
import threading

from confluent_kafka import Consumer, KafkaError

from .processor_context import ProcessorContext

log = logging.getLogger(__name__)

class StreamTask:
    def __init__(self, task_id, application_id, partitions, topology_builder, consumer):
        self.task_id = task_id
        self.application_id = application_id
        self.partitions = partitions
        self.topology = topology_builder.build()
        self.consumer = consumer

        self.queue = queue.Queue()
        self.context = ProcessorContext()

        self._init_topology(self.context)

    def _init_topology(self, context):
        for node in self.topology.nodes.values():
            try:
                context.currentNode = node
                node.initialise(context)
            finally:
                context.currentNode = None

    def add_records(self, partition, records):
        for record in records:
            self.queue.put(record)

    def process(self):
        if self.queue.empty():
            return False

        record = self.queue.get()

        self.context.currentNode = self.topology.sources[0] # TODO: FIXME-  assumes only one topic
        self.topology.sources[0].process(record.key(), record.value()) # TODO: FIXME - assumes only one topic

class StreamThread:
    def __init__(self, _topology, _config):
        super().__init__()
        self.topology = _topology
        self.config = _config

        self.tasks = []
        self._running = True

        self.topics = _topology.topics

        log.info('Topics for consumer are: %s', self.topics)
        # TODO: read values from config
        self.consumer = Consumer({'bootstrap.servers': self.config.BOOTSTRAP_SERVERS, 'group.id': 'testgroup',
                                  'default.topic.config': {'auto.offset.reset': self.config.AUTO_OFFSET_RESET}})
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
