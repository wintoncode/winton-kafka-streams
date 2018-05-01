"""
Record collector sends produced results to kafka topic

"""

import logging

from ..errors.kafka_streams_error import KafkaStreamsError

log = logging.getLogger(__name__)

# When producing a message with partition = UA rdkafka will run a partitioner for us
RD_KAFKA_PARTITION_UA = -1


class RecordCollector:
    """
    Collects records to be output to Kafka topics after
    they have been processed by the topology

    """

    def __init__(self, _producer, _key_serde, _value_serde):
        self.producer = _producer
        self.key_serde = _key_serde
        self.value_serde = _value_serde

    def send(self, topic, key, value, timestamp,
             *, partition=RD_KAFKA_PARTITION_UA, partitioner=None):
        ser_key = self.key_serde.serializer.serialize(topic, key)
        ser_value = self.value_serde.serializer.serialize(topic, value)
        produced = False

        log.debug("Sending to partition %d of topic %s :  (%s, %s, %s)", partition, topic, ser_key, ser_value, timestamp)

        while not produced:
            try:
                self.producer.produce(topic, ser_value, ser_key, partition, self.on_delivery, partitioner, timestamp)
                self.producer.poll(0)  # Ensure previous message's delivery reports are served
                produced = True
            except BufferError as be:
                log.exception(be)
                self.producer.poll(10)  # Wait a bit longer to give buffer more time to flush
            except NotImplementedError as nie:
                log.exception(nie)
                produced = True  # should not enter infinite loop

    def on_delivery(self, err, msg):
        """
        Callback function after a value is output to a source.

        Will raise an exception if an error is detected.

        TODO: Decide if an error should be raised or if this should be demoted?
              Can an error be raised if a broker fails? Should we simply warn
              and continue to poll and retry in this case?
        """

        # TODO: Is err correct? Should we check if msg has error?
        if err:
            raise KafkaStreamsError(f'Error on delivery of message {msg}')

    def flush(self):
        """
        Flush all pending items in the queue to the output topic on Kafka

        """
        log.debug('Flushing producer')
        self.producer.flush()

    def close(self):
        log.debug('Closing producer')
        self.producer.close()
