from winton_kafka_streams.state import ChangeLoggingKeyValueStore
from abc import ABC, abstractmethod


class BaseStorageKeyValueStoreFactory(ABC):
    def __init__(self, name, key_serde, value_serde):
        self.name = name
        self.key_serde = key_serde
        self.value_serde = value_serde
        self.logging_enabled = True

    def enable_logging(self, config_map):
        # TODO changelog extra config gets handled here
        self.logging_enabled = True
        return self

    def disable_logging(self):
        self.logging_enabled = False
        return self

    def _wrap_storage_dict(self, storage_dict):
        storage = storage_dict
        if self.logging_enabled:
            storage = ChangeLoggingKeyValueStore(self.name, storage)
        return storage

    @abstractmethod
    def build(self):
        pass
