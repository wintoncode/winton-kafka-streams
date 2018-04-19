"""
Json Serde

"""
from .._json import JsonSerializer, JsonDeserializer
from .wrapper_serde import WrapperSerde


class JsonSerde(WrapperSerde):
    def __init__(self):
        serializer = JsonSerializer()
        deserializer = JsonDeserializer()
        super().__init__(serializer, deserializer)
