"""
Wall clock time extractor

"""

import time

from ._timestamp import TimeStampExtractor


class WallClockTimeStampExtractor(TimeStampExtractor):
    """
    Time stamp extractor that returns wall clock time at the point
    a record is processed
    """

    def extract(self, record, previous_timestamp):
        """
        Returns wall clock time for every message

        Parameters:
        -----------
        record : Kafka record
            New record from which time should be assigned
        previous_timestamp : long
            Last extracted timestamp (seconds since the epoch)

        Returns:
        --------
        time : long
            Time in seconds since the epoch
        """
        return time.time()
