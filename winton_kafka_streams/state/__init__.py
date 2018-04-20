from winton_kafka_streams.state.factory.store_factory import StoreFactory


def create(name: str) -> StoreFactory:
    # TODO replace this Java-esque factory with a Pythonic DSL as part of the other work on a Streams DSL
    return StoreFactory(name)
