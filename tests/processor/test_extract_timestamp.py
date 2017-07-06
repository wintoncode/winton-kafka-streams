"""
Tests of using wall clock time from a message

"""

import unittest.mock as mock
import pytest
import time

import winton_kafka_streams.processor as wks_processor


expected_time = 1496735099.23712

def test_RecordTimeStampExtractorNoImpl():
    pytest.raises(TypeError, wks_processor.RecordTimeStampExtractor)


class RecordTimeStampExtractorImpl(wks_processor.RecordTimeStampExtractor):
    def on_error(self, record, timestamp, previous_timestamp):
        return time.time()

def test_RecordTimeStampExtractor():
    rtse = RecordTimeStampExtractorImpl()
    assert rtse.extract(wks_processor.KafkaRecord(_topic='TestTopic', _partition=None, _offset=1234, _key=None, _value=None, _timestamp=expected_time), expected_time-1) == expected_time

def test_InvalidRecordTiemStampExtractorNoImpl():
    on_error_time = expected_time - 1000
    rtse = RecordTimeStampExtractorImpl()
    with mock.patch('time.time', return_value=on_error_time):
        assert rtse.extract(wks_processor.KafkaRecord(_topic='TestTopic', _partition=None, _offset=1234, _key=None, _value=None, _timestamp=-1), expected_time-1) == on_error_time
