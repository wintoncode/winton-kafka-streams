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

    context = wks_processor.processor_context.ProcessorContext()
    topology.nodes['my-processor-1'].initialise(context)
    topology.nodes['my-processor-2'].initialise(context)


class TestTopology(unittest.TestCase):
    def setUp(self):
        self.topology = wks_processor.topology.TopologyBuilder()

    def test_source(self):
        self.topology.source('my-source', ['my-input-topic-1'])

    def test_processor(self):
        self.topology.source('my-source', ['my-input-topic-1'])
        self.topology.processor('my-processor', MyTestProcessor, 'my-source')

        assert len(self.topology.nodes) == 2
        assert 'my-source' in self.topology.nodes.keys()
        assert 'my-processor' in self.topology.nodes.keys()

    def test_sink(self):
        self.topology.source('my-source', ['my-input-topic-1'])
        self.topology.processor('my-processor', MyTestProcessor, 'my-source')
        self.topology.sink('my-sink', 'my-output-topic-1', 'my-processor')

        assert len(self.topology.nodes) == 3
        assert 'my-source' in self.topology.nodes.keys()
        assert 'my-processor' in self.topology.nodes.keys()
        assert 'my-sink' in self.topology.nodes.keys()


    def test_addNoneState(self):
        pytest.raises(wks_error.KafkaStreamsError, self.topology.add_state_store, None)

    def test_addSameStateTwice(self):
        store = wks_state.SimpleStore('my-simple-state')
        self.topology.add_state_store(store)
        # Second attempt to add same state raises
        pytest.raises(wks_error.KafkaStreamsError, self.topology.add_state_store, store)

    def test_addStateAndConnectInOneCall(self):
        _create_full_topology(self.topology)

        store = wks_state.SimpleStore('my-simple-state')
        self.topology.add_state_store(store, 'my-processor-1')

    def test_connectStateToNoneProcessors(self):
        _create_full_topology(self.topology)

        store = wks_state.SimpleStore('my-simple-state')
        self.topology.add_state_store(store, 'my-processor-1')

        pytest.raises(wks_error.KafkaStreamsError, self.topology.connect_processor_to_store, 'my-simple-state')

    def test_assertExceptionOnSourceConnect(self):
        _create_full_topology(self.topology)

        store = wks_state.SimpleStore('my-simple-state')
        self.topology.add_state_store(store, 'my-processor-1')

        pytest.raises(wks_error.KafkaStreamsError, self.topology.connect_processor_to_store, 'my-source', 'my-simple-state')


    def test_assertExceptionOnSinkConnect(self):
        _create_full_topology(self.topology)

        store = wks_state.SimpleStore('my-simple-state')
        pytest.raises(wks_error.KafkaStreamsError, self.topology.connect_processor_to_store, 'my-sink', 'my-simple-state')


    def test_addStateAndThenConnectState(self):
        _create_full_topology(self.topology)

        store = wks_state.SimpleStore('my-simple-state')
        self.topology.add_state_store(store)

        # Separate connection step
        self.topology.connect_processor_to_store('my-processor-1', 'my-simple-state')

        assert len(self.topology.nodes['my-processor-1'].stores) == 1
        assert 'my-simple-state' in self.topology.nodes['my-processor-1'].stores
