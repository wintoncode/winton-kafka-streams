"""
Test of topology creation

Low level connection of processor units
"""

import pytest
import unittest

import winton_kafka_streams._error as wks_error
import winton_kafka_streams.processor as wks_processor
import winton_kafka_streams.state as wks_state


def test_createTopologyBuilder():
    wks_processor.topology.TopologyBuilder()


class MyTestProcessor(wks_processor.processor.BaseProcessor):
    pass


def _create_full_topology(topology):

    topology.source('my-source', ['my-input-topic-1'])
    topology.processor('my-processor-1', MyTestProcessor, 'my-source')
    topology.processor('my-processor-2', MyTestProcessor, 'my-source')
    topology.sink('my-sink', 'my-output-topic-1', 'my-processor-1', 'my-processor-2')
    topology.state_store('my-simple-state', lambda: wks_state.SimpleStore('my-simple-state'), ['my-processor-1'])

    return topology.build()


class TestTopology(unittest.TestCase):
    def setUp(self):
        self.topology = wks_processor.topology.TopologyBuilder()

    def test_source(self):
        self.topology.source('my-source', ['my-input-topic-1'])

    def test_processor(self):
        self.topology.source('my-source', ['my-input-topic-1'])
        self.topology.processor('my-processor', MyTestProcessor, 'my-source')

        self.topology = self.topology.build()

        assert len(self.topology.nodes) == 2
        assert 'my-source' in self.topology.nodes.keys()
        assert 'my-processor' in self.topology.nodes.keys()

    def test_sink(self):
        self.topology.source('my-source', ['my-input-topic-1'])
        self.topology.processor('my-processor', MyTestProcessor, 'my-source')
        self.topology.sink('my-sink', 'my-output-topic-1', 'my-processor')

        self.topology = self.topology.build()

        assert len(self.topology.nodes) == 3
        assert 'my-source' in self.topology.nodes.keys()
        assert 'my-processor' in self.topology.nodes.keys()
        assert 'my-sink' in self.topology.nodes.keys()
