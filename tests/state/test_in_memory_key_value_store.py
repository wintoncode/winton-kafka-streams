import pytest

from winton_kafka_streams.processor.serialization.serdes import BytesSerde
from winton_kafka_streams.state.in_memory.in_memory_state_store import InMemoryStateStore


def test_inMemoryKeyValueStore():
    store = InMemoryStateStore('teststore', BytesSerde(), BytesSerde(), False)
    kv_store = store.get_key_value_store()

    kv_store['a'] = 1
    assert kv_store['a'] == 1

    kv_store['a'] = 2
    assert kv_store['a'] == 2

    del kv_store['a']
    assert kv_store.get('a') is None
    with pytest.raises(KeyError):
        _ = kv_store['a']
