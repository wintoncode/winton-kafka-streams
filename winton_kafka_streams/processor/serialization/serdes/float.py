"""
Float Serde

"""
from ..float import FloatDeserializer, FloatSerializer
from ._wrapper_serde import WrapperSerde


class FloatSerde(WrapperSerde):
    def __init__(self):
        serializer = FloatSerializer()
        deserializer = FloatDeserializer()
        super().__init__(serializer, deserializer)
