"""
Wall clock time extractor

"""

import time

from ._timestamp import TimeStampExtractor

class WallClockTimeStampExtractor(TimeStampExtractor):
    """
    Returns wall clock time for every message

    """
    def extract(self, value):
        return time.gmtime()
