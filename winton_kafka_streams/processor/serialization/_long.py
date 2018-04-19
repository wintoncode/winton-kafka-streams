from ._integer import IntegerSerializer, IntegerDeserializer


class LongSerializer(IntegerSerializer):
    def __init__(self):
        super(LongSerializer, self).__init__()
        self.int_size = 8


class LongDeserializer(IntegerDeserializer):
    def __init__(self):
        super(LongDeserializer, self).__init__()
