"""
Test the top-level Kafka Streams class
"""


import pytest
import unittest.mock as mock

from winton_kafka_streams import kafka_config
from winton_kafka_streams.errors.kafka_streams_error import KafkaStreamsError
from winton_kafka_streams.kafka_streams import KafkaStreams
from winton_kafka_streams.processor.processor import BaseProcessor
from winton_kafka_streams.processor.topology import TopologyBuilder


class MyTestProcessor(BaseProcessor):
    pass


def test__given__stream_already_started__when__call_start_again__then__raise_error():
    kafka_config.NUM_STREAM_THREADS = 0
    topology_builder = TopologyBuilder()

    topology_builder.source('my-source', ['my-input-topic-1'])
    topology_builder.processor('my-processor', MyTestProcessor, 'my-source')
    topology_builder.sink('my-sink', 'my-output-topic-1', 'my-processor')

    topology = topology_builder.build()

    kafka_streams = KafkaStreams(topology, kafka_config)
    kafka_streams.start()

    with pytest.raises(KafkaStreamsError, message='KafkaStreams already started.'):
        kafka_streams.start()


def test__two__processes__with__two__topic__partitions():
    NUM_STREAM_PROCESSES = 2
    kafka_config.NUM_STREAM_THREADS = 1

    consumer = mock.Mock()
    producer = mock.Mock()

    processor_attrs = {'process.return_value': None}
    processor = mock.Mock(**processor_attrs)

    kafka_client_supplier_attrs = {'consumer.return_value': consumer,
                                   'producer.return_value': producer}
    kafka_client_supplier = mock.Mock(**kafka_client_supplier_attrs)

    topology_builder = TopologyBuilder()

    topology_builder.source('my-source', ['my-input-topic-1'])
    topology_builder.processor('my-processor', processor, 'my-source')
    topology_builder.sink('my-sink', 'my-output-topic-1', 'my-processor')

    with mock.patch('winton_kafka_streams.kafka_client_supplier.KafkaClientSupplier', return_value=kafka_client_supplier):
        for partition in range(NUM_STREAM_PROCESSES):
            kafka_stream_process = KafkaStreams(topology_builder, kafka_config)

            topic_partition_attrs = {'topic': 'testtopic',
                                     'partition': partition}
            topic_partition = mock.Mock(**topic_partition_attrs)

            kafka_stream_process.stream_threads[0].add_stream_tasks([topic_partition])

            record_attrs = {'topic.return_value': 'my-input-topic-1',
                            'offset.return_value': 1,
                            'partition.return_value': partition}
            record = mock.Mock(**record_attrs)

            kafka_stream_process.stream_threads[0].add_records_to_tasks([record])
