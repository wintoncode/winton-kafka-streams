from ._serde import extract_config_property
from ._deserializer import Deserializer
from ._serializer import Serializer


class StringSerializer(Serializer):
    def __init__(self):
        self.encoding = 'utf-8'
        self.on_error = 'strict'

    def serialize(self, topic, data):
        return data.encode(self.encoding, self.on_error)

    def configure(self, configs, is_key):
        encoding_value = extract_config_property(configs, is_key, 'serializer.encoding')
        if encoding_value:
            self.encoding = encoding_value

        error_value = extract_config_property(configs, is_key, 'serializer.error')
        if error_value:
            self.on_error = error_value

    def close(self):
        pass


class StringDeserializer(Deserializer):
    def __init__(self):
        self.encoding = 'utf-8'
        self.on_error = 'strict'

    def deserialize(self, topic, data):
        return data.decode(self.encoding, self.on_error)

    def configure(self, configs, is_key):
        encoding_value = extract_config_property(configs, is_key, 'deserializer.encoding')
        if encoding_value:
            self.encoding = encoding_value

        error_value = extract_config_property(configs, is_key, 'deserializer.error')
        if error_value:
            self.on_error = error_value

    def close(self):
        pass
