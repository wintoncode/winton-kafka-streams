from ._serde import extract_config_property
from ._deserializer import Deserializer
from ._serializer import Serializer


class IntegerSerializer(Serializer):
    def __init__(self):
        self.byte_order = 'little'
        self.signed = True
        self.int_size = 8

    def serialize(self, topic, data):
        return int(data).to_bytes(length=self.int_size, byteorder=self.byte_order, signed=self.signed)

    def configure(self, configs, is_key):
        self.byte_order = extract_config_property(configs, is_key, 'SERIALIZER_BYTEORDER')
        self.signed = extract_config_property(configs, is_key, 'SERIALIZER_SIGNED').lower() == 'true'
        self.int_size = extract_config_property(configs, is_key, 'SERIALIZER_INT_SIZE')

    def close(self):
        pass


class IntegerDeserializer(Deserializer):
    def __init__(self):
        self.byte_order = 'little'
        self.signed = True

    def deserialize(self, topic, data):
        return int.from_bytes(bytes=data, byteorder=self.byte_order, signed=self.signed)

    def configure(self, configs, is_key):
        self.byte_order = extract_config_property(configs, is_key, 'DESERIALIZER_BYTEORDER')
        self.signed = extract_config_property(configs, is_key, 'DESERIALIZER_SIGNED').lower() == 'true'

    def close(self):
        pass
