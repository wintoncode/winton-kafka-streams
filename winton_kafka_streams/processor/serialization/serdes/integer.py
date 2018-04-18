"""
Integer Serde

"""
from ..integer import IntegerDeserializer, IntegerSerializer
from .wrapper_serde import WrapperSerde


class IntegerSerde(WrapperSerde[int]):
    def __init__(self):
        serializer = IntegerSerializer()
        deserializer = IntegerDeserializer()
        super().__init__(serializer, deserializer)
