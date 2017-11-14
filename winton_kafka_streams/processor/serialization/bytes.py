from ._deserializer import Deserializer
from ._serializer import Serializer


class BytesSerializer(Serializer):
    def serialize(self, topic, data):
        return data

    def configure(self, configs, is_key):
        pass

    def close(self):
        pass


class BytesDeserializer(Deserializer):
    def deserialize(self, topic, data):
        return data

    def configure(self, configs, is_key):
        pass

    def close(self):
        pass
