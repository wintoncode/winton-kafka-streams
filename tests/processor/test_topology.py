"""
Test of topology creation

Low level connection of processor units
"""


import unittest
import winton_kafka_streams.processor as wks_processor

def test_createTopologyBuilder():
    wks_processor.topology.TopologyBuilder()


class MyTestProcessor(wks_processor.processor.BaseProcessor):
    pass


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
