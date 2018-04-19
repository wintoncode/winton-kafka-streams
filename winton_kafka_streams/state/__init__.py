from winton_kafka_streams.state.factory.store_factory import StoreFactory


def create(name: str) -> StoreFactory:
    return StoreFactory(name)
