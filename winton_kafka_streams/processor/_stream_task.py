import logging
import queue

from confluent_kafka import TopicPartition
from confluent_kafka.cimpl import KafkaException, KafkaError

from winton_kafka_streams.processor.serialization.serdes import BytesSerde
from ..errors._kafka_error_codes import _get_invalid_producer_epoch_code
from ._punctuation_queue import PunctuationQueue
from ._record_collector import RecordCollector
from .processor_context import ProcessorContext
from .wallclock_timestamp import WallClockTimeStampExtractor
from ..errors.task_migrated_error import TaskMigratedError
from ..processor.serialization.serdes import serde_from_string


class DummyRecord:
    """
    Dummy implementation of Record that provides the minimum needed
    to supply a timestamp to Context during punctuate.
    """

    def __init__(self, timestamp):
        self._timestamp = timestamp

    def topic(self):
        return '__null_topic__'

    def partition(self):
        return -1

    def offset(self):
        return -1

    def timestamp(self):
        return self._timestamp




_taskMigratedErrorCodes = [KafkaError.ILLEGAL_GENERATION,
                           KafkaError.REBALANCE_IN_PROGRESS,
                           KafkaError.UNKNOWN_MEMBER_ID,
                           _get_invalid_producer_epoch_code()]


class StreamTask:
    """
    Stream tasks are associated with a partition group(s)
    and are responsible for passing values from that partition
    to an instance of the topology for processing.

    """

    def __init__(self, _task_id, _application_id, _partitions, _topology_builder, _consumer, _producer, _config):
        self.log = logging.getLogger(__name__ + '(' + str(_task_id) + ')')
        self.task_id = _task_id
        self.application_id = _application_id
        self.partitions = _partitions
        self.topology = _topology_builder.build()
        self.state_stores = {name: store.get() for name, store in self.topology.state_stores.items()}
        self.consumer = _consumer
        self.producer = _producer
        self.config = _config

        self.key_serde = serde_from_string(self.config.KEY_SERDE)
        self.key_serde.configure(self.config, True)
        self.value_serde = serde_from_string(self.config.VALUE_SERDE)
        self.value_serde.configure(self.config, False)

        self.record_collector = RecordCollector(self.producer, self.key_serde, self.value_serde)
        self.state_record_collector = RecordCollector(self.producer, BytesSerde(), BytesSerde())

        self.queue = queue.Queue()
        self.context = ProcessorContext(self.task_id, self, self.record_collector,
                                        self.state_record_collector, self.state_stores)

        self.punctuation_queue = PunctuationQueue(self.punctuate)
        # TODO: use the configured timestamp extractor.
        self.timestamp_extractor = WallClockTimeStampExtractor()
        self.current_timestamp = None

        self.commitRequested = False
        self.commitOffsetNeeded = False
        self.consumedOffsets = {}

        self._init_state_stores()
        self._init_topology(self.context)

    def _init_state_stores(self):
        self.log.debug(f'Initialising state stores')
        for store in self.state_stores.values():
            store.initialize(self.context, store)

    def _init_topology(self, context):
        for node in self.topology.nodes.values():
            try:
                context.current_node = node
                node.initialise(context)
            finally:
                context.current_node = None
                context.current_record = None

    def add_records(self, records):
        for record in records:
            self.queue.put(record)

    def process(self):
        if self.queue.empty():
            return False

        record = self.queue.get()
        self.context.current_record = record
        self.current_timestamp = self.timestamp_extractor.extract(record, self.current_timestamp)

        topic = record.topic()
        raw_key = record.key()
        key = None if raw_key is None else self.key_serde.deserializer.deserialize(topic, record.key())
        value = self.value_serde.deserializer.deserialize(topic, record.value())

        self.context.current_node = self.topology.sources[topic]
        self.topology.sources[topic].process(key, value)

        self.consumedOffsets[(topic, record.partition())] = record.offset()
        self.commitOffsetNeeded = True

        self.context.current_record = None
        self.context.current_node = None

        return True

    def maybe_punctuate(self):
        timestamp = self.current_timestamp

        if timestamp is None:
            return False

        return self.punctuation_queue.may_punctuate(timestamp)

    def punctuate(self, node, timestamp):
        self.log.debug(f'Punctuating processor {node} at {timestamp}')
        self.context.current_record = DummyRecord(timestamp)
        self.context.current_node = node
        node.punctuate(timestamp)
        self.context.current_record = None
        self.context.current_node = None

    def commit(self):
        try:
            self.record_collector.flush()
            self.commit_offsets()
            self.commitRequested = False
        except Exception as e:
            self.log.exception(e)
            raise

    def commit_offsets(self):
        """ Commit consumed offsets if needed """

        # may be asked to commit on rebalance or shutdown but
        # should only commit if the processor has requested.
        try:
            if self.commitOffsetNeeded:
                offsets_to_commit = [TopicPartition(t, p, o + 1) for ((t, p), o) in self.consumedOffsets.items()]
                self.consumer.commit(offsets=offsets_to_commit, asynchronous=False)
                self.consumedOffsets.clear()
                self.commitOffsetNeeded = False

        except KafkaException as ke:
            kafka_error = ke.args[0].code()

            if kafka_error in _taskMigratedErrorCodes:
                raise TaskMigratedError(f'{self} migrated.')
            else:
                raise

    def commit_needed(self):
        return self.commitRequested

    def need_commit(self):
        self.commitRequested = True

    def schedule(self, interval):
        self.punctuation_queue.schedule(self.context.current_node, interval)

    def __repr__(self):
        return self.__class__.__name__ + f":{self.task_id}"
