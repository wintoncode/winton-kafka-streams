"""
Identity serde (default)

"""
from ..identity import IdentitySerializer, IdentityDeserializer
from ._wrapper_serde import WrapperSerde


class IdentitySerde(WrapperSerde):
    """
    Identity serializer that makes no changes to values
    during serialization deserialization
    """

    def __init__(self):
        serializer = IdentitySerializer()
        deserializer = IdentityDeserializer()
        super().__init__(serializer, deserializer)
