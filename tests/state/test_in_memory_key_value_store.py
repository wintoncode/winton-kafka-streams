from winton_kafka_streams.state.in_memory_key_value_store import InMemoryKeyValueStore


def test_inMemoryKeyValueStore():
    store = InMemoryKeyValueStore('teststore')

    store['a'] = 1
    assert store['a'] == 1

    store['a'] = 2
    assert store['a'] == 2
