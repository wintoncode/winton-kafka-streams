"""
Classes for building a graph topology comprising processor derived nodes

"""

from .processor import SourceProcessor, SinkProcessor
from .._error import KafkaStreamsError


class ProcessorNode:
    def __init__(self, _name, _processor):
        self.name = _name
        self.processor = _processor
        self.children = []

    def initialise(self, _context):
        self.processor.initialise(self.name, _context)

    def process(self, key, value):
        self.processor.process(key, value)

    def punctuate(self, timestamp):
        self.processor.punctuate(timestamp)

    def __repr__(self):
        return self.__class__.__name__ + f"({self.processor.__class__}({self.name}))"

    def add_store(self, store):
        self.processor.context.add_store(store.name, store)


class TopologyBuilder:
    """
    Convenience class for building a graph topology
    """
    def __init__(self):
        self.nodes = {}
        self.state_stores = {}

        self._sources = []
        self._sinks = []

    def _add_node(self, name, processor, inputs=[]):
        if name in self.nodes:
            raise KafkaStreamsError("A processor with the name '%s' already added to this topology", name)
        self.nodes[name] = processor

        node_inputs = list(self.nodes[i] for i in inputs)

        if any(n.name == name for n in node_inputs):
            raise KafkaStreamsError("A processor cannot have itself as an input")
        if any(n.name not in self.nodes for n in node_inputs):
            raise KafkaStreamsError("Input(s) {} to processor {} do not yet exist" \
                .format((set(inputs) - set(n.name for i in node_inputs)), name))

        for i in inputs:
            self.nodes[i].children.append(processor)

    @property
    def sources(self):
        return self._sources

    @property
    def sinks(self):
        return sefl._sinks

    def add_state_store(self, store, *args):
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
            self.connect_processor_to_store(procesor_name, store.name)


    def connect_processor_to_store(self, processor_name, *args):
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

        processor_node = self.nodes.get(processor_name)
        if processor_node is None:
            raise KafkaStreamsError(f"Unrecognised processor name {processor_name} passed to connect state store")
        if isinstance(processor_node.processor, (SourceProcessor, SinkProcessor)):
            raise KafkaStreamsError(f"{processor_node.__class__.__name__} type cannot have a state store attached")

        for store_name in args:
            if store_name not in self.state_stores:
                raise KafkaStreamsError(f"Unknown store {store_name} being added to {processor_name}")
            processor_node.add_store(self.state_stores[store_name])

    def source(self, name, topics):
        processor_node = ProcessorNode(name, SourceProcessor(topics))
        self._add_node(name, processor_node, [])
        self._sources.append(processor_node)
        return processor_node

    def processor(self, name, processor, *inputs):
        """
        Add a processor to the topology

        Parameters:
        -----------
        name : str
            The name of the node
        processor : winton_kafka_streams.processor.base.BaseProcessor
            Processor object that will process the key, value pair passed
        *inputs:
            Parent nodes supplying inputs

        Returns:
        --------
        topology : TopologyBuilder

        Raises:
        KafkaStreamserror
            * If no inputs are specified
        """
        if not inputs:
            raise KafkaStreamsError("Processor '%s' must have a minimum of 1 input", name)
        processor_node = ProcessorNode(name, processor)
        self._add_node(name, processor_node, inputs)
        return processor_node

    def sink(self, name, topic, *inputs):
        processor_node = ProcessorNode(name, SinkProcessor(topic))
        self._add_node(name, processor_node, inputs)
        self._sinks.append(processor_node)
        return processor_node

    def pprint(self, out):
        """
        Pretty print a topology

        Parameters:
        ----------
        out : ostream
        """

        node_char = {
            'SourceProcessor' : '*',
            'SinkProcessor' : '>'
        }

        # TODO: Improve
        stack = [[ n for n in self.nodes.values() if isinstance(n.processor, SourceProcessor)]]
        while stack and stack[-1]:
            child = stack[-1].pop()
            out.write('  '*(len(stack)-1)+'{} '.format(node_char.get(child.processor.__class__.__name__, '|')) + child.name+'\n')
            if child.children:
                stack.append(child.children)
            if not stack[-1]:
                del stack[-1]
