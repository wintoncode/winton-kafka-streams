from winton_kafka_streams.state.factory.store_factory import StoreFactory


def create(name):
    return StoreFactory(name)
