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

        self.offsets = []

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
        produced = False
        while not produced:
            try:
                self.producer.produce(topic, value, key, partition, self.on_delivery, timestamp)
                self.producer.poll(0) # Ensure previous message's delivery reports are served
                produced = True
            except BufferError as be:
                log.exception(be)
                self.producer.poll(10) # Wait a bit longer to give buffer more time to flush
            except NotImplementedError as nie:
                log.exception(nie)
                produced = True  # should not enter infinite loop

    def on_delivery(self, err, msg):
        if err:
            raise KafkaStreamsError(f'Error on delivery of message {msg}')

    def flush(self):
        log.debug('Flushing producer')
        self.producer.flush()

    def close(self):
        log.debug('Closing producer')
        self.producer.close()

    def offsets(self):
        """
        The last ack'ed offsets from the topology producer

        Returns:
        --------
        offsets : list
          The last acknowledged offsets
        """
        return self.offsets
