"""
Kafka consumer poll thread

TODO: Definitely very rough and ready!

"""

import threading

from confluent_kafka import Consumer, KafkaError


class KafkaConsumerThread(threading.Thread):
    #daemon = True

    def __init__(self, _topics, _context, _src):
        super().__init__()

        self.topics = _topics
        self.context = _context

        self.src = _src # TODO: DEBUG - not correct way to pass source processor

        self.consumer = Consumer({'bootstrap.servers': 'localhost', 'group.id': 'testroup',
                    'default.topic.config': {'auto.offset.reset': 'smallest'}})
        self.consumer.subscribe(self.topics)

    def run(self):
        while True:
            msg = self.consumer.poll()
            if not msg.error():
                print('Received message: %s' % msg.value().decode('utf-8'))
                try:
                    self.src.process("", float(msg.value().decode('utf-8')))
                except ValueError as ve:
                    print(str(ve))
                    return # DEBUG: easy way to break out
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            elif msg.error():
                print(msg.error())

    def close(self):
        self.consumer.close()

def start_consumer(topics, context, src):
    kct = KafkaConsumerThread(topics, context, src)
    kct.start()

    return kct