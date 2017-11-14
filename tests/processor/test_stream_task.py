"""
StreamTask tests
"""

from unittest.mock import Mock, patch

import pytest
from confluent_kafka.cimpl import KafkaError, KafkaException

from winton_kafka_streams import kafka_config
from winton_kafka_streams.errors.task_migrated_error import TaskMigratedError
from winton_kafka_streams.processor import TopologyBuilder
from winton_kafka_streams.processor._stream_task import StreamTask
from winton_kafka_streams.processor.task_id import TaskId

taskMigratedErrorCodes = [KafkaError.ILLEGAL_GENERATION,
                          KafkaError.UNKNOWN_MEMBER_ID,
                          KafkaError.REBALANCE_IN_PROGRESS,
                          47 # INVALID_PRODUCER_EPOCH - not supported in all versions for Conluent Kafka so just use the explicit code in this test
                          ]


@pytest.mark.parametrize("error_code", taskMigratedErrorCodes)
def test__given__commit__when__consumer_commit_fails_as_task_migrated__then__throw_task_migrated_error(error_code):
    kafka_error_attrs = {'code.return_value': error_code}
    kafka_error = Mock(**kafka_error_attrs)

    with patch.object(KafkaException, 'args', [kafka_error]):
        consumer_attrs = {'commit.side_effect': KafkaException()}
        consumer = Mock(**consumer_attrs)
        producer = Mock()
        processor_attrs = {'process.return_value': None}
        processor = Mock(**processor_attrs)

        topology_builder = TopologyBuilder()

        topology_builder.source('my-source', ['my-input-topic-1'])
        topology_builder.processor('my-processor', processor, 'my-source')
        topology_builder.sink('my-sink', 'my-output-topic-1', 'my-processor')

        task = StreamTask(TaskId('testgroup', 0), "myapp", [0], topology_builder, consumer, producer, kafka_config)

        record_attrs = {'topic.return_value': 'my-input-topic-1',
                        'offset.return_value': 1,
                        'partition.return_value': 0}
        record = Mock(**record_attrs)

        task.add_records([record])

        task.process()

        with pytest.raises(TaskMigratedError, message='StreamTask:testgroup_0 migrated.'):
            task.commit()
