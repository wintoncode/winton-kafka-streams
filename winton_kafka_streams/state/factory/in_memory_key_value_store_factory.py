from winton_kafka_streams.state.factory.base_storage_key_value_store_factory import BaseStorageKeyValueStoreFactory
from winton_kafka_streams.state.in_memory.in_memory_state_store import InMemoryStateStoreSupplier


class InMemoryKeyValueStoreFactory(BaseStorageKeyValueStoreFactory):
    def __init__(self, name, key_serde, value_serde):
        super(InMemoryKeyValueStoreFactory, self).__init__(name, key_serde, value_serde)

    def build(self):
        return InMemoryStateStoreSupplier(self.name, self.key_serde, self.value_serde, self.logging_enabled)
