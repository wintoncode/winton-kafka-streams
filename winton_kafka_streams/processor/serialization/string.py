from ._serde import extract_config_property
from ._deserializer import Deserializer
from ._serializer import Serializer


class StringSerializer(Serializer[str]):
    def __init__(self):
        self.encoding = 'utf-8'
        self.on_error = 'strict'

    def serialize(self, topic: str, data: str) -> bytes:
        return str(data).encode(self.encoding, self.on_error)

    def configure(self, configs, is_key):
        self.encoding = extract_config_property(configs, is_key, 'SERIALIZER_ENCODING')
        self.on_error = extract_config_property(configs, is_key, 'SERIALIZER_ERROR')

    def close(self):
        pass


class StringDeserializer(Deserializer[str]):
    def __init__(self):
        self.encoding = 'utf-8'
        self.on_error = 'strict'

    def deserialize(self, topic: str, data: bytes) -> str:
        return data.decode(self.encoding, self.on_error)

    def configure(self, configs, is_key):
        self.encoding = extract_config_property(configs, is_key, 'DESERIALIZER_ENCODING')
        self.on_error = extract_config_property(configs, is_key, 'DESERIALIZER_ERROR')

    def close(self):
        pass
