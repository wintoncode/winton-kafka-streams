from .deserializer import Deserializer
from .serializer import Serializer


class BytesSerializer(Serializer[bytes]):
    def serialize(self, topic: str, data: bytes) -> bytes:
        return data

    def configure(self, configs, is_key):
        pass

    def close(self):
        pass


class BytesDeserializer(Deserializer[bytes]):
    def deserialize(self, topic: str, data: bytes) -> bytes:
        return data

    def configure(self, configs, is_key):
        pass

    def close(self):
        pass
