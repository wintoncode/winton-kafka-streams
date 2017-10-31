from ._deserializer import Deserializer
from ._serializer import Serializer
import struct


class FloatSerializer(Serializer):
    def serialize(self, topic, data):
        return struct.pack('f', data)

    def configure(self, configs, is_key):
        pass

    def close(self):
        pass


class FloatDeserializer(Deserializer):
    def deserialize(self, topic, data):
        return struct.unpack('f', data)

    def configure(self, configs, is_key):
        pass

    def close(self):
        pass
