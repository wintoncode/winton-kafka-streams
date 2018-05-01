from .deserializer import Deserializer
from .serializer import Serializer
import struct


class FloatSerializer(Serializer[float]):
    def serialize(self, topic: str, data: float) -> bytes:
        return struct.pack('f', data)

    def configure(self, configs, is_key):
        pass

    def close(self):
        pass


class FloatDeserializer(Deserializer[float]):
    def deserialize(self, topic: str, data: bytes) -> float:
        return struct.unpack('f', data)[0]

    def configure(self, configs, is_key):
        pass

    def close(self):
        pass
