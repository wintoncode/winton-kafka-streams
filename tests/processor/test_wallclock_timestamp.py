"""
Tests of using wall clock time from a message

"""

import mock
import pytest
import time

import winton_kafka_streams.processor.wallclock_timestamp as wallclock_timestamp

def test_WallClockTimeStampExtractor():
    expected_time = 1496735099.23712

    with mock.patch('time.gmtime', return_value=expected_time):
        assert wallclock_timestamp.WallClockTimeStampExtractor().extract(None) == expected_time
