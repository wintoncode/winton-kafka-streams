"""
Test the top-level Kafka Streams class
"""


import pytest

from winton_kafka_streams import kafka_config
from winton_kafka_streams.errors.kafka_streams_error import KafkaStreamsError
from winton_kafka_streams.kafka_streams import KafkaStreams
from winton_kafka_streams.processor.processor import BaseProcessor
from winton_kafka_streams.processor.topology import TopologyBuilder


class MyTestProcessor(BaseProcessor):
    pass


def test_Given_StreamAlreadyStarted_When_CallStartAgain_Then_RaiseError():
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
