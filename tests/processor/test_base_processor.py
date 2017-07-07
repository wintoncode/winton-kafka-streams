"""
Test the base processor - base class to all
custom processor implementations
"""

import winton_kafka_streams.processor as wks_processor
from winton_kafka_streams.processor.processor_context import ProcessorContext

def test_createBaseProcessor():
    wks_processor.BaseProcessor()

def test_initialiseBaseProcessor():
    mock_context = ProcessorContext(None, None, {})
    bp = wks_processor.BaseProcessor()
    bp.initialise('my-name', mock_context)

    assert bp.name == 'my-name'
    assert isinstance(bp.context, ProcessorContext)
