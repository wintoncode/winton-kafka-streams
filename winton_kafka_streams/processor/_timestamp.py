"""
Base class for all timestamp extractors

"""

import abc


class TimeStampExtractor(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def extract(self, record, previous_timestamp):
        pass
