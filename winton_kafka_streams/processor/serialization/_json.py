from ._string import StringSerializer, StringDeserializer
from .deserializer import Deserializer
from .serializer import Serializer
import json


class JsonSerializer(Serializer):
    def __init__(self):
        self.string_serializer = StringSerializer()

    def serialize(self, topic, data):
        string_form = json.dumps(data)
        return self.string_serializer.serialize(topic, string_form)

    def configure(self, configs, is_key):
        self.string_serializer.configure(configs, is_key)

    def close(self):
        pass


class JsonDeserializer(Deserializer):
    def __init__(self):
        self.string_deserializer = StringDeserializer()

    def deserialize(self, topic, data):
        string_form = self.string_deserializer.deserialize(topic, data)
        return json.loads(string_form)

    def configure(self, configs, is_key):
        self.string_deserializer.configure(configs, is_key)

    def close(self):
        pass
