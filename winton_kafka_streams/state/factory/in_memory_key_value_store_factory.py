from winton_kafka_streams.state.factory.base_storage_key_value_store_factory import BaseStorageKeyValueStoreFactory


class InMemoryKeyValueStoreFactory(BaseStorageKeyValueStoreFactory):
    def __init__(self, name, key_serde, value_serde):
        super(InMemoryKeyValueStoreFactory, self).__init__(name, key_serde, value_serde)
