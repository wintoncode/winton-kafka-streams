"""
Tests of using wall clock time from a message

"""

import unittest.mock as mock

import winton_kafka_streams.processor.wallclock_timestamp as wallclock_timestamp

expected_time = 1496735099.23712


def test_WallClockTimeStampExtractor():
    with mock.patch('time.time', return_value=expected_time):
        assert wallclock_timestamp.WallClockTimeStampExtractor().extract(None, expected_time-1) == expected_time
