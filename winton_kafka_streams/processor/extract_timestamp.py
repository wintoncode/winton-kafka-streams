"""
Time extractor from the message being processed

"""

import abc

from ._timestamp import TimeStampExtractor


class RecordTimeStampExtractor(TimeStampExtractor):
    """
    Time stamp extractor that returns a time taken from the message itself

    This is an abstract class, the on_error function must be implemented to
    use this extractor.
    """

    def extract(self, record, previous_timestamp):
        """
        Returns kafka timestamp for message

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
        (timestamp_type, timestamp) = record.timestamp()
        if timestamp < 0:
            return self.on_error(record, timestamp, previous_timestamp)

        return timestamp

    @abc.abstractmethod
    def on_error(self, record, timestamp, previous_timestamp):
        """
        Called when an invalid timestamp is found in a record

        Parameters:
        record : Kafka record
            The current record being processed
        timestamp : long
            The (invalid) timestamp that was processed
        previous_timestamp : long
            Last extracted timestamp (seconds since the epoch)
        """
        pass
