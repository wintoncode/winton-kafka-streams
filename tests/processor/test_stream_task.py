"""
StreamTask tests
"""


import pytest
from unittest.mock import Mock, patch

from confluent_kafka.cimpl import KafkaError, KafkaException

from winton_kafka_streams.errors.task_migrated_error import TaskMigratedError
from winton_kafka_streams.processor import TopologyBuilder, BaseProcessor
from winton_kafka_streams.processor._stream_task import StreamTask, DummyRecord
from winton_kafka_streams.processor.task_id import TaskId

taskMigratedErrorCodes = [KafkaError.ILLEGAL_GENERATION,
                          KafkaError.UNKNOWN_MEMBER_ID,
                          KafkaError.REBALANCE_IN_PROGRESS,
                          KafkaError.INVALID_PRODUCER_EPOCH]


@pytest.mark.parametrize("errorCode", taskMigratedErrorCodes)
def test_Given_Commit_When_ConsumerCommitFailsAsTaskMigrated_Then_ThrowTaskMigratedError(errorCode):
    kafkaErrorAttrs = {'code.return_value': errorCode}
    kafkaError = Mock(**kafkaErrorAttrs)

    with patch.object(KafkaException, 'args', [kafkaError]):
        consumerAttrs = {'commit.side_effect': KafkaException()}
        consumer = Mock(**consumerAttrs)
        producer = Mock()
        processorAttrs = {'process.return_value': None}
        processor = Mock(**processorAttrs)

        topology_builder = TopologyBuilder()

        topology_builder.source('my-source', ['my-input-topic-1'])
        topology_builder.processor('my-processor', processor, 'my-source')
        topology_builder.sink('my-sink', 'my-output-topic-1', 'my-processor')

        task = StreamTask(TaskId('testgroup', 0), "myapp", [0], topology_builder, consumer, producer)

        recordAttrs = {'topic.return_value': 'my-input-topic-1',
                       'offset.return_value': 1,
                       'partition.return_value': 0}
        record = Mock(**recordAttrs)

        task.add_records([record])

        task.process()

        with pytest.raises(TaskMigratedError, message='StreamTask:testgroup_0 migrated.'):
            task.commit()

