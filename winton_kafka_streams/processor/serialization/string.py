from ._deserializer import Deserializer
from ._serializer import Serializer


def _extract_property(configs, is_key, property_name):
    prop_value = ''
    overridden_property_name = ('key.%s' % property_name) if is_key else ('value.%s' % property_name)
    if overridden_property_name in configs:
        prop_value = configs[overridden_property_name]
    elif property_name in configs:
        prop_value = configs[property_name]
    return prop_value


class StringSerializer(Serializer):
    def __init__(self):
        self.encoding = 'utf-8'
        self.on_error = 'strict'

    def serialize(self, topic, data):
        return data.encode(self.encoding, self.on_error)

    def configure(self, configs, is_key):
        encoding_value = _extract_property(configs, is_key, 'serializer.encoding')
        if encoding_value:
            self.encoding = encoding_value

        error_value = _extract_property(configs, is_key, 'serializer.error')
        if error_value:
            self.on_error = error_value

    def close(self):
        pass


class StringDeserializer(Deserializer):
    def __init__(self):
        self.encoding = 'utf-8'
        self.on_error = 'strict'

    def close(self):
        pass

    def deserialize(self, topic, data):
        return data.decode(self.encoding, self.on_error)

    def configure(self, configs, is_key):
        encoding_value = _extract_property(configs, is_key, 'deserializer.encoding')
        if encoding_value:
            self.encoding = encoding_value

        error_value = _extract_property(configs, is_key, 'deserializer.error')
        if error_value:
            self.on_error = error_value
