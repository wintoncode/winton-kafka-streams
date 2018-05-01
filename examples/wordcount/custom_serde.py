from winton_kafka_streams.processor.serialization import IntegerSerializer
from winton_kafka_streams.processor.serialization.serdes.wrapper_serde import WrapperSerde
from winton_kafka_streams.processor.serialization import StringDeserializer


class StringIntSerde(WrapperSerde):
    def __init__(self):
        serializer = IntegerSerializer()
        deserializer = StringDeserializer()
        super().__init__(serializer, deserializer)
