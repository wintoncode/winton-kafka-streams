from .deserializer import Deserializer
from .serializer import Serializer
import struct


class DoubleSerializer(Serializer[float]):
    def serialize(self, topic: str, data: float) -> bytes:
        return struct.pack('d', data)

    def configure(self, configs, is_key):
        pass

    def close(self):
        pass


class DoubleDeserializer(Deserializer[float]):
    def deserialize(self, topic: str, data: bytes) -> float:
        return struct.unpack('d', data)[0]

    def configure(self, configs, is_key):
        pass

    def close(self):
        pass
