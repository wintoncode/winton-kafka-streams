from ._serde import extract_config_property
from ._deserializer import Deserializer
from ._serializer import Serializer
import struct


class FloatSerializer(Serializer):
    def __init__(self):
        self.double = True

    def serialize(self, topic, data):
        fmt_str = 'd' if self.double else 'f'
        return struct.pack(fmt_str, data)

    def configure(self, configs, is_key):
        self.double = extract_config_property(configs, is_key, 'SERIALIZER_DOUBLE_PRECISION').lower() == 'true'

    def close(self):
        pass


class FloatDeserializer(Deserializer):
    def __init__(self):
        self.double = True

    def deserialize(self, topic, data):
        fmt_str = 'd' if self.double else 'f'
        return struct.unpack(fmt_str, data)[0]

    def configure(self, configs, is_key):
        self.double = extract_config_property(configs, is_key, 'DESERIALIZER_DOUBLE_PRECISION').lower() == 'true'

    def close(self):
        pass
