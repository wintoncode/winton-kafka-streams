"""
Test the base processor - base class to all
custom processor implementations
"""

import unittest.mock as mock

import winton_kafka_streams.processor as wks_processor
from winton_kafka_streams.processor.processor_context import ProcessorContext
from winton_kafka_streams.processor.task_id import TaskId


def test_createBaseProcessor():
    wks_processor.BaseProcessor()


def test_initialiseBaseProcessor():
    mock_task = mock.Mock()
    mock_task.application_id = 'test_id'
    mock_task_id = TaskId('test_group', 0)
    mock_context = ProcessorContext(mock_task_id, mock_task, None, None, {})
    bp = wks_processor.BaseProcessor()
    bp.initialise('my-name', mock_context)

    assert bp.name == 'my-name'
    assert isinstance(bp.context, ProcessorContext)
