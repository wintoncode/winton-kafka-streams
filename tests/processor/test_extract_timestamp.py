"""
Tests of using wall clock time from a message

"""

import unittest.mock as mock
import pytest

import winton_kafka_streams.processor as wks_processor

expected_time = 1496735099.23712
error_time_offset = 1000
timestamp_create_time = 1


class TestRecordTimeStampExtractorImpl(wks_processor.RecordTimeStampExtractor):
    def on_error(self, record, timestamp, previous_timestamp):
        return timestamp - 1000


class MockRecord:
    def __init__(self, time):
        self.time = time

    def timestamp(self):
        return (timestamp_create_time, self.time)


def test_RecordTimeStampExtractorNoImpl():
    pytest.raises(TypeError, wks_processor.RecordTimeStampExtractor)


def test_RecordTimeStampExtractor():
    rtse = TestRecordTimeStampExtractorImpl()
    assert rtse.extract(MockRecord(expected_time), expected_time-12345) == expected_time


def test_InvalidRecordTimeStampExtractorNoImpl():
    rtse = TestRecordTimeStampExtractorImpl()
    assert rtse.extract(MockRecord(-1), expected_time-12345) == -1 - error_time_offset
