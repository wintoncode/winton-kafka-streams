"""
Test of source processor behaviour
"""

import winton_kafka_streams.processor as wks_processor


def test_createSourceProcessorObject():
    wks_processor.SourceProcessor(['test-topic-name'])


def test_sourceProcessorTopic():
    sp1 = wks_processor.SourceProcessor(('topic1',))
    assert sp1.topics == ('topic1',)
    sp2 = wks_processor.SourceProcessor(('topic1', 'topic2'))
    assert sp2.topics == ('topic1', 'topic2')
