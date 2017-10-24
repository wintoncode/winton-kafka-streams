from ._deserializer import Deserializer
from ._serializer import Serializer


class IdentitySerializer(Serializer):
    def serialize(self, topic, data):
        return data

    def configure(self, configs, is_key):
        pass

    def close(self):
        pass


class IdentityDeserializer(Deserializer):
    def close(self):
        pass

    def deserialize(self, topic, data):
        return data

    def configure(self, configs, is_key):
        pass
