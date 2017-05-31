"""
Base class for all timestamp extractors

"""

import abc

class TimeStampExtractor(object):
    @abc.abstractmethod
    def extract(self, value):
        pass