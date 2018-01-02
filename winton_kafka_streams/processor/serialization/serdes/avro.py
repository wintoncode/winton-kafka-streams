"""
Avro Serde

"""
from ..avro import AvroSerializer, AvroDeserializer
from ._wrapper_serde import WrapperSerde


class AvroSerde(WrapperSerde):
    """
    Avro Serde that will use Avro and a schema registry
    for serialization and deserialization
    """

    def __init__(self):
        serializer = AvroSerializer()
        deserializer = AvroDeserializer()
        super().__init__(serializer, deserializer)
