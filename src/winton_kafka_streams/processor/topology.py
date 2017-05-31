"""
Classes for building a graph topology comprising processor derived nodes

"""

from .base import SourceProcessor, SinkProcessor, Processor
from .._error import KafkaStreamsError


class TopologyBuilder(object):
    def __init__(self):        
        self.nodes = {}

    def _add_node(self, name, node, inputs):
        if name in self.nodes:
            raise KafkaStreamsError("A processor with the name '%s' already added to this topology", name)
        self.nodes[name] = node

        for i in inputs:
            try:
                node.downstream.append(self.nodes[i])
            except KeyError:
                raise KafkaStreamsError("Node '%s' declares input '%s' but no input node with this name exists.", name, i)

    def source(self, name, *topics):
        self._add_node(name, SourceProcessor(*topics), [])
        return self

    def processor(self, name, transform, inputs):
        if not inputs:
            raise KafkaStreamsError("Processor '%s' must have a minimum of 1 input", name)
        self._add_node(name, Processor(transform), inputs)
        return self

    def sink(self, name, topic, transform, inputs):
        if not inputs:
            raise KafkaStreamsError("Processor '%s' must have a minimum of 1 input", name)
        self._add_node(name, SinkProcessor(topic, transform), inputs)
