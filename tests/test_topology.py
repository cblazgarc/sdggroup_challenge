"""Tests for engine.topology: cycle detection + topological sort over the combined data+wait graph."""
import pytest

from engine.graph import build_graph
from engine.metadata_schema import MetadataFile
from engine.topology import GraphCycleError, topological_sort
from tests.fixtures import clone_valid_metadata


def _build_prueba_acceso_graph():
    metadata = MetadataFile.model_validate(clone_valid_metadata())
    return build_graph(metadata.dataflows[0])


def test_real_dataflow_has_no_cycle_and_respects_precedence():
    graph = _build_prueba_acceso_graph()

    order = topological_sort(graph)

    assert set(order) == set(graph.nodes.keys())
    position = {name: i for i, name in enumerate(order)}

    # data-edge precedence: producer strictly before consumer
    for producer, consumer in graph.data_edges:
        assert position[producer] < position[consumer], f"{producer} should precede {consumer}"

    # wait-edge precedence: waited node strictly before the waiting input
    for waited, waiting in graph.wait_edges:
        assert position[waited] < position[waiting], f"{waited} should precede {waiting}"


def test_cycle_purely_within_data_edges_is_detected():
    # a -> b -> a (both transformations), no waits involved at all
    data = {
        "dataflows": [
            {
                "name": "cyclic",
                "inputs": [
                    {"name": "raw", "type": "file", "config": {"path": "/in", "format": "csv"}}
                ],
                "transformations": [
                    {"name": "a", "type": "filter", "input": "b", "config": {"filter": "1=1"}},
                    {"name": "b", "type": "filter", "input": "a", "config": {"filter": "1=1"}},
                ],
                "outputs": [
                    {
                        "name": "out",
                        "type": "file",
                        "input": "a",
                        "config": {"path": "/out", "format": "parquet", "save_mode": "overwrite"},
                    }
                ],
            }
        ]
    }
    metadata = MetadataFile.model_validate(data)
    graph = build_graph(metadata.dataflows[0])

    with pytest.raises(GraphCycleError) as exc_info:
        topological_sort(graph)

    cycle = exc_info.value.cycle
    assert cycle[0] == cycle[-1]
    assert set(cycle[:-1]) == {"a", "b"}


def test_cycle_only_appears_by_combining_data_and_wait_edges():
    # input A waits=[B]; output B's input=A -> A -> B (data edge B depends... )
    # Concretely: A (input, waits=[B]); B (output, input=A). Data edge A->B,
    # wait edge B->A: combined graph has a 2-cycle A->B->A even though
    # neither edge type alone forms a cycle.
    data = {
        "dataflows": [
            {
                "name": "cross-branch-cycle",
                "inputs": [
                    {
                        "name": "A",
                        "type": "file",
                        "config": {"path": "/in", "format": "csv"},
                        "waits": ["B"],
                    }
                ],
                "transformations": [],
                "outputs": [
                    {
                        "name": "B",
                        "type": "file",
                        "input": "A",
                        "config": {"path": "/out", "format": "parquet", "save_mode": "overwrite"},
                    }
                ],
            }
        ]
    }
    metadata = MetadataFile.model_validate(data)
    graph = build_graph(metadata.dataflows[0])

    # neither edge type alone is cyclic
    assert graph.data_edges == [("A", "B")]
    assert graph.wait_edges == [("B", "A")]

    with pytest.raises(GraphCycleError) as exc_info:
        topological_sort(graph)

    cycle = exc_info.value.cycle
    assert cycle[0] == cycle[-1]
    assert set(cycle[:-1]) == {"A", "B"}


def test_disconnected_components_are_all_included_in_the_order():
    data = {
        "dataflows": [
            {
                "name": "two-branches",
                "inputs": [
                    {"name": "in1", "type": "file", "config": {"path": "/in1", "format": "csv"}},
                    {"name": "in2", "type": "file", "config": {"path": "/in2", "format": "csv"}},
                ],
                "transformations": [],
                "outputs": [
                    {
                        "name": "out1",
                        "type": "file",
                        "input": "in1",
                        "config": {"path": "/out1", "format": "parquet", "save_mode": "overwrite"},
                    },
                    {
                        "name": "out2",
                        "type": "file",
                        "input": "in2",
                        "config": {"path": "/out2", "format": "parquet", "save_mode": "overwrite"},
                    },
                ],
            }
        ]
    }
    metadata = MetadataFile.model_validate(data)
    graph = build_graph(metadata.dataflows[0])

    order = topological_sort(graph)

    assert set(order) == {"in1", "in2", "out1", "out2"}
    assert order.index("in1") < order.index("out1")
    assert order.index("in2") < order.index("out2")
