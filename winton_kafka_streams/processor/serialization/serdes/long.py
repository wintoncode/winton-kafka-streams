"""
Long Serde

"""
from ..long import LongDeserializer, LongSerializer
from .wrapper_serde import WrapperSerde


class LongSerde(WrapperSerde[int]):
    def __init__(self):
        serializer = LongSerializer()
        deserializer = LongDeserializer()
        super().__init__(serializer, deserializer)
