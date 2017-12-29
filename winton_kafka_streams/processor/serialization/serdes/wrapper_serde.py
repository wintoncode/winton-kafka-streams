"""
Serde from a Serializer and Deserializer

"""

from .._serde import Serde


class WrapperSerde(Serde):

    def __init__(self, serializer, deserializer):
        self.serializer = serializer
        self.deserializer = deserializer

    def configure(self, configs, is_key):
        self.serializer.configure(configs, is_key)
        self.deserializer.configure(configs, is_key)

    def serializer(self):
        return self.serializer

    def deserializer(self):
        return self.deserializer

    def close(self):
        self.serializer.close()
        self.deserializer.close()
