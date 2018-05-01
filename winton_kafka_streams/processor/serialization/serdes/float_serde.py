"""
Float Serde

"""
from .._float import FloatDeserializer, FloatSerializer
from .wrapper_serde import WrapperSerde


class FloatSerde(WrapperSerde[float]):
    def __init__(self):
        serializer = FloatSerializer()
        deserializer = FloatDeserializer()
        super().__init__(serializer, deserializer)
