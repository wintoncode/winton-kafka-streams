"""
Bytes Serde (default)

"""
from .._bytes import BytesSerializer, BytesDeserializer
from .wrapper_serde import WrapperSerde


class BytesSerde(WrapperSerde[bytes]):
    """
    Bytes Serde that makes no changes to values
    during serialization or deserialization
    """

    def __init__(self):
        serializer = BytesSerializer()
        deserializer = BytesDeserializer()
        super().__init__(serializer, deserializer)
