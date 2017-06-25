"""
Classes for building a graph topology comprising processor derived nodes

"""

import logging
import functools

from .processor import SourceProcessor, SinkProcessor
from .._error import KafkaStreamsError

log = logging.getLogger(__name__)

class ProcessorNode:
    def __init__(self, _name, _processor):
        self.name = _name
        self.processor = _processor
        self.children = []
        self.stores = {}

    def initialise(self, _context):
        self.processor.initialise(self.name, _context)

    def process(self, key, value):
        self.processor.process(key, value)

    def punctuate(self, timestamp):
        self.processor.punctuate(timestamp)

    def __repr__(self):
        return self.__class__.__name__ + f"({self.processor.__class__}({self.name}))"

    def add_store(self, store):
        log.debug(f"Processor Node {self.name} now has state {store.name} added")
        self.stores[store.name] = store


class Topology:
    def __init__(self, _nodes, _sources, _processors, _sinks):
        self.nodes = _nodes
        self.sources = _sources
        self.processors = _processors
        self.sinks = _sinks

class TopologyBuilder:
    """
    Convenience class for building a graph topology
    """
    def __init__(self):
        self.topics = []
        self._sources = []
        self._processors = []
        self._sinks = []
        self.state_stores = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _add_node(self, nodes, name, processor, inputs=[]):
        if name in nodes:
            raise KafkaStreamsError(f"A processor with the name '{name}' already added to this topology")
        nodes[name] = processor

        node_inputs = list(nodes[i] for i in inputs)

        if any(n.name == name for n in node_inputs):
            raise KafkaStreamsError("A processor cannot have itself as an input")
        if any(n.name not in nodes for n in node_inputs):
            raise KafkaStreamsError("Input(s) {} to processor {} do not yet exist" \
                .format((set(inputs) - set(n.name for i in node_inputs)), name))

        for i in inputs:
            nodes[i].children.append(processor)

    @property
    def sources(self):
        return self._sources

    @property
    def sinks(self):
        return self._sinks

    def _add_state_store(self, nodes, store, *args):
        """
        Add a store and connect to processors

        Parameters:
        -----------
        store : winton_kafka_streams.processor.store.AbstractStore
            State store instance
        *args : processor names to which store should be attached

        Raises:
        KafkaStreamsError
            * If store is None
            * If store already exists
        """
        if store is None:
            raise KafkaStreamsError("Store cannot be None")

        if store.name in self.state_stores:
            raise KafkaStreamsError(f"Store with name {store.name} already exists")
        self.state_stores[store.name] = store

        for procesor_name in args:
            self._connect_processor_to_store(nodes, procesor_name, store.name)


    def _connect_processor_to_store(self, nodes, processor_name, *args):
        """
        Connect state store instance(s) to a processor

        Parameters:
        -----------
        processor_name : str
            The object to which the state store(s)
            are attached
        *args : winton_kafka_streams.processor.store.AbstractStore
            State store instance(s)

        Raises:
        KafkaStreamsError
            * If processor_name is unset
            * If a state store is attached to a source or sink
        """
        if not processor_name:
            raise KafkaStreamsError("Processor name cannot be empty")

        processor_node = nodes.get(processor_name)
        if processor_node is None:
            raise KafkaStreamsError(f"Unrecognised processor name {processor_name} passed to connect state store")
        if isinstance(processor_node.processor, (SourceProcessor, SinkProcessor)):
            raise KafkaStreamsError(f"{processor_node.__class__.__name__} type cannot have a state store attached")

        for store_name in args:
            log.debug(f"Adding store {store_name} to {processor_name}")
            if store_name not in self.state_stores:
                raise KafkaStreamsError(f"Unknown store {store_name} being added to {processor_name}")
            processor_node.add_store(self.state_stores[store_name])

    def source(self, name, topics):
        def build_source(name, topics, nodes):
            source = ProcessorNode(name, SourceProcessor(topics))
            self._add_node(nodes, name, source, [])
            return source

        self._sources.append(functools.partial(build_source, name, topics))
        self.topics.extend(topics)
        return self

    def processor(self, name, processor_type, *parents, stores=[]):
        """
        Add a processor to the topology

        Parameters:
        -----------
        name : str
            The name of the node
        processor : winton_kafka_streams.processor.base.BaseProcessor
            Processor object that will process the key, value pair passed
        *parents:
            Parent nodes supplying inputs

        Returns:
        --------
        topology : TopologyBuilder

        Raises:
        KafkaStreamserror
            * If no inputs are specified
        """
        if not parents:
            raise KafkaStreamsError("Processor '%s' must have a minimum of 1 input", name)

        def build_processor(name, processor_type, parents, stores, nodes):
            processor_node = ProcessorNode(name, processor_type())
            self._add_node(nodes, name, processor_node, parents)
            if stores:
                for store in stores:
                    self._add_state_store(nodes, store, name)
            return processor_node

        self._processors.append(functools.partial(build_processor, name, processor_type, parents, stores))
        return self

    def sink(self, name, topic, *parents):
        def build_sink(name, topic, parents, nodes):
            sink = ProcessorNode(name, SinkProcessor(topic))
            self._add_node(nodes, name, sink, parents)
            return sink
        self._sinks.append(functools.partial(build_sink, name, topic, parents))
        return self

    def build(self):
        nodes = {}
        sources = [source_builder(nodes) for source_builder in self._sources]
        processors = [processor_builder(nodes) for processor_builder in self._processors]
        sinks = [sink_builder(nodes) for sink_builder in self._sinks]

        return Topology(nodes, sources, processors, sinks)
