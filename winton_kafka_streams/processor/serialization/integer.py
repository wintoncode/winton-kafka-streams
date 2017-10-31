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
        byte_order = extract_config_property(configs, is_key, 'serializer.byte_order')
        if byte_order:
            self.byte_order = byte_order

        signed = extract_config_property(configs, is_key, 'serializer.signed')
        if signed:
            self.byte_order = signed

        int_size = extract_config_property(configs, is_key, 'serializer.int_size')
        if int_size:
            self.int_size = int_size

    def close(self):
        pass


class IntegerDeserializer(Deserializer):
    def __init__(self):
        self.byte_order = 'little'
        self.signed = True

    def deserialize(self, topic, data):
        return int.from_bytes(bytes=data, byteorder=self.byte_order, signed=self.signed)

    def configure(self, configs, is_key):
        byte_order = extract_config_property(configs, is_key, 'deserializer.byte_order')
        if byte_order:
            self.byte_order = byte_order

        signed = extract_config_property(configs, is_key, 'deserializer.signed')
        if signed:
            self.signed = signed

    def close(self):
        pass
