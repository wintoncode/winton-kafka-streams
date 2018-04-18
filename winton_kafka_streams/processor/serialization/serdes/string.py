"""
String Serde

"""
from ..string import StringSerializer, StringDeserializer
from ..bytes import BytesSerializer, BytesDeserializer
from .wrapper_serde import WrapperSerde


class StringSerde(WrapperSerde[str]):
    def __init__(self):
        serializer = StringSerializer()
        deserializer = StringDeserializer()
        super().__init__(serializer, deserializer)
