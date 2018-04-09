import pytest
from winton_kafka_streams.state.in_memory.in_memory_state_store import InMemoryKeyValueStore


def test_inMemoryKeyValueStore():
    store = InMemoryKeyValueStore('teststore')

    store['a'] = 1
    assert store['a'] == 1

    store['a'] = 2
    assert store['a'] == 2

    del store['a']
    assert store.get('a') is None
    with pytest.raises(KeyError):
        store['a']
