"""
"""

class KafkaRecord:
    """
    A record from Kafka
    """

    def __init__(self, *, _topic, _partition, _offset, _key, _value, _timestamp):
        self.topic = _topic
        self.partition = _partition
        self.offset = _offset
        self.key = _key
        self.value = _value
        self.timestamp = _timestamp
