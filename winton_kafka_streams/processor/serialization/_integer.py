from .serde import extract_config_property
from .deserializer import Deserializer
from .serializer import Serializer


class IntegerSerializer(Serializer[int]):
    def __init__(self):
        self.byte_order = 'little'
        self.signed = True
        self.int_size = 4

    def serialize(self, topic: str, data: int) -> bytes:
        return int(data).to_bytes(length=self.int_size, byteorder=self.byte_order, signed=self.signed)

    def configure(self, configs, is_key):
        self.byte_order = extract_config_property(configs, is_key, 'SERIALIZER_BYTEORDER', self.byte_order)
        self.signed = extract_config_property(configs, is_key, 'SERIALIZER_SIGNED', str(self.signed)).lower() == 'true'

    def close(self):
        pass


class IntegerDeserializer(Deserializer[int]):
    def __init__(self):
        self.byte_order = 'little'
        self.signed = True

    def deserialize(self, topic: str, data: bytes) -> int:
        return int.from_bytes(bytes=data, byteorder=self.byte_order, signed=self.signed)

    def configure(self, configs, is_key):
        self.byte_order = extract_config_property(configs, is_key, 'DESERIALIZER_BYTEORDER', self.byte_order)
        self.signed = extract_config_property(configs, is_key, 'DESERIALIZER_SIGNED', str(self.signed)).lower() == 'true'

    def close(self):
        pass
