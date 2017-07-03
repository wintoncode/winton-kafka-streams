"""
Record collector sends produced results to kafka topic

"""

import time

from .._error import KafkaStreamsError

class DefaultStreamPartitioner:
    def __init__(self):
        pass

    def partition(self):
        return 0

class IdentitySerialiser:
    def serialise(self, value):
        return value

class RecordCollector:
    def __init__(self, _producer):
        self.producer = _producer

    def send_to_stream(self, topic, key, value, timestamp, keySerialiser, valueSerialiser,
                       *, stream_partitioner = DefaultStreamPartitioner()):

        partitions = producer.partitionsFor(topic)
        n_partitions = len(partitions)
        if n_partitions == 0:
            raise KafkaStreamsError(f"Could not get partition information for {topic}." \
                                    "This can happen if the topic does not exist.")

        self.send_to_partition(topic, key, value, timestamp, keySerialiser,
                               valueSerialiser, partition = partitioner.partition(key, value, n_partitions))

    def send_to_partition(self, topic, key, value, timestamp,
                          keySerialiser = IdentitySerialiser(), valueSerialiser = IdentitySerialiser(), *, partition = 0):
        key = keySerialiser.serialise(key)
        value = valueSerialiser.serialise(value)
        try:
            self.producer.produce(topic, value, key, partition, self.on_delivery, timestamp)
            self.producer.poll(0) # Ensure previous message's delivery reports are served
        except BufferError as be:
            # TODO: We should handle this better. The queue can be full, for example on leader election and we should retry
            log.exception(be)
        except NotImplementedError as nie:
            log.exception(nie)

    def on_delivery(self, err, msg):
        if err:
            raise KafkaStreamsError(f'Error on delivery of message {msg}')

    def flush(self):
        log.debug('Flushing producer')
        self.producer.flush()

    def close(self):
        log.debug('Closing producer')
        self.producer.close()
