"""
Classes for building a graph topology comprising processor derived nodes

"""

import logging

from .processor import SourceProcessor, SinkProcessor
from .._error import KafkaStreamsError

log = logging.getLogger(__name__)

class ProcessorNode:
    def __init__(self, _name, _processor):
        self.name = _name
        self.processor = _processor
        self.children = []
        self.states = set()

    def initialise(self, _context):
        self.processor.initialise(self.name, _context)

    def process(self, key, value):
        self.processor.process(key, value)

    def punctuate(self, timestamp):
        self.processor.punctuate(timestamp)

    def __repr__(self):
        return self.__class__.__name__ + f"({self.processor.__class__}({self.name}))"


class Topology:
    def __init__(self, _nodes, _sources, _processors, _sinks, _state_stores):
        self.nodes = _nodes
        self.sources = _sources
        self.processors = _processors
        self.sinks = _sinks
        self.state_stores = _state_stores

class TopologyBuilder:
    """
    Convenience class for building a graph topology
    """
    def __init__(self):
        self.topics = []
        self._sources = []
        self._processors = []
        self._sinks = []
        self._state_stores = []

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

    @property
    def state_stores(self):
        return self._state_stores

    def state_store(self, store_name, store_type, *processors):
        """
        Add a store and connect to processors

        Parameters:
        -----------
        store : winton_kafka_streams.processor.store.AbstractStore
            State store factory
        *processors : processor names to which store should be attached

        Raises:
        KafkaStreamsError
            * If store is None
            * If store already exists
        """
        if store_type is None:
            raise KafkaStreamsError("Store cannot be None")

        if any(store_name == s.name() for s in self._state_stores):
            raise KafkaStreamsError(f"Store with name {store.name} already exists")

        def build_store():
            return store_type(store_name), processors
        def _name():
            return store_name
        build_store.name = _name

        self._state_stores.append(build_store)

    def source(self, name, topics):
        """
        Add a source to the topology

        Parameters:
        -----------
        name : str
            The name of the node
        topics : str
            Source topic

        Returns:
        --------
        topology : TopologyBuilder

        Raises:
        KafkaStreamsError
            * If node with same name exists already
        """

        def build_source(nodes):
            source = ProcessorNode(name, SourceProcessor(topics))
            self._add_node(nodes, name, source, [])
            return source

        self._sources.append(build_source)
        self.topics.extend(topics)
        return self

    def processor(self, name, processor_type, *parents):
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
        KafkaStreamsError
            * If no inputs are specified
        """
        if not parents:
            raise KafkaStreamsError("Processor '%s' must have a minimum of 1 input", name)

        def build_processor(nodes):
            processor_node = ProcessorNode(name, processor_type())
            self._add_node(nodes, name, processor_node, parents)
            return processor_node

        self._processors.append(build_processor)
        return self

    def sink(self, name, topic, *parents):
        def build_sink(nodes):
            sink = ProcessorNode(name, SinkProcessor(topic))
            self._add_node(nodes, name, sink, parents)
            return sink
        self._sinks.append(build_sink)
        return self

    def build(self):
        nodes = {}
        sources = [source_builder(nodes) for source_builder in self._sources]
        processors = [processor_builder(nodes) for processor_builder in self._processors]
        sinks = [sink_builder(nodes) for sink_builder in self._sinks]

        state_stores = {}
        for state_builder in self._state_stores:
            (state_stores[state_builder.name()], processors) = state_builder()
            for p in processors:
                nodes[p].states.add(state_builder.name())

        return Topology(nodes, sources, processors, sinks, state_stores)
