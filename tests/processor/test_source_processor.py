"""
Test of source processor behaviour
"""

import winton_kafka_streams.processor as wks_processor

def test_createSourceProcessorObject():
    wks_processor.SourceProcessor()

def test_sourceProcessorTopic():
    sp1 = wks_processor.SourceProcessor('topic1')
    assert sp1.topic == ('topic1',)
    sp2 = wks_processor.SourceProcessor('topic1', 'topic2')
    assert sp2.topic == ('topic1', 'topic2')