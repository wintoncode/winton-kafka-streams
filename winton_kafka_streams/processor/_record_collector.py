"""
Record collector sends produced results to kafka topic

"""

import time
import logging

from .serde.identity import IdentitySerde
from .._error import KafkaStreamsError

log = logging.getLogger(__name__)


# When producing a message with partition = UA rdkafka will run a partitioner for us
RD_KAFKA_PARTITION_UA = -1


class RecordCollector:
    """
    Collects records to be output to Kafka topics after
    they have been processed by the topology

    """
    def __init__(self, _producer):
        self.producer = _producer

    def send(self, topic, key, value, timestamp,
             keySerialiser = IdentitySerde(), valueSerialiser = IdentitySerde(),
             *, partition = RD_KAFKA_PARTITION_UA, partitioner = None):
        key = keySerialiser.serialise(key)
        value = valueSerialiser.serialise(value)
        produced = False

        log.debug("Sending to partition %d of topic %s :  (%s, %s, %s)", partition, topic, key, value, timestamp)

        while not produced:
            try:
                self.producer.produce(topic, value, key, partition, self.on_delivery, partitioner, timestamp)
                self.producer.poll(0) # Ensure previous message's delivery reports are served
                produced = True
            except BufferError as be:
                log.exception(be)
                self.producer.poll(10) # Wait a bit longer to give buffer more time to flush
            except NotImplementedError as nie:
                log.exception(nie)
                produced = True  # should not enter infinite loop

    def on_delivery(self, err, msg):
        """
        Callback function after a value is output to a source.

        Will raise an exception if an error is detected.

        TODO: Decide if an error should be raised or if this should be demoted?
              Can an error be raised if a broker fails? Should we simply warn
              and continue to poll and retrty in this case?
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
