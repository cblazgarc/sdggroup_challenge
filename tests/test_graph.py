"""Tests for engine.graph: DAG structure built from a validated Dataflow."""
from engine.graph import NodeKind, build_graph, build_graphs
from engine.metadata_schema import MetadataFile
from tests.fixtures import clone_valid_metadata


def _build_prueba_acceso_graph():
    metadata = MetadataFile.model_validate(clone_valid_metadata())
    return build_graph(metadata.dataflows[0])


def test_build_graphs_returns_one_graph_per_dataflow():
    metadata = MetadataFile.model_validate(clone_valid_metadata())

    graphs = build_graphs(metadata)

    assert set(graphs.keys()) == {"prueba-acceso"}
    assert graphs["prueba-acceso"].dataflow_name == "prueba-acceso"


def test_every_node_is_present_with_the_right_kind():
    graph = _build_prueba_acceso_graph()

    expected_kinds = {
        "demo_data": NodeKind.INPUT,
        "parquet_data": NodeKind.INPUT,
        "filter_rows": NodeKind.TRANSFORMATION,
        "new_fields": NodeKind.TRANSFORMATION,
        "group_by_fields": NodeKind.TRANSFORMATION,
        "write_last_file": NodeKind.OUTPUT,
        "write_historic_file": NodeKind.OUTPUT,
        "write_delta_merge": NodeKind.OUTPUT,
        "write_delta_raw": NodeKind.OUTPUT,
    }

    assert set(graph.nodes.keys()) == set(expected_kinds.keys())
    for name, expected_kind in expected_kinds.items():
        assert graph.nodes[name].kind == expected_kind


def test_data_edges_are_built_from_input_fields():
    graph = _build_prueba_acceso_graph()

    expected_data_edges = {
        ("demo_data", "new_fields"),
        ("new_fields", "filter_rows"),
        ("parquet_data", "group_by_fields"),
        ("filter_rows", "write_last_file"),
        ("filter_rows", "write_historic_file"),
        ("group_by_fields", "write_delta_merge"),
        ("group_by_fields", "write_delta_raw"),
    }

    assert set(graph.data_edges) == expected_data_edges
    assert len(graph.data_edges) == len(expected_data_edges)


def test_wait_edges_are_kept_separate_from_data_edges():
    graph = _build_prueba_acceso_graph()

    assert graph.wait_edges == [("write_last_file", "parquet_data")]
    # the wait must never leak into the data-edge list
    assert ("write_last_file", "parquet_data") not in graph.data_edges


def test_input_nodes_expose_waits_but_not_data_inputs():
    graph = _build_prueba_acceso_graph()

    parquet_data = graph.nodes["parquet_data"]
    demo_data = graph.nodes["demo_data"]

    assert parquet_data.data_inputs == ()
    assert parquet_data.waits == ("write_last_file",)
    assert demo_data.data_inputs == ()
    assert demo_data.waits == ()


def test_transformation_and_output_nodes_never_have_waits():
    graph = _build_prueba_acceso_graph()

    for name in ("filter_rows", "new_fields", "group_by_fields", "write_last_file", "write_delta_merge"):
        assert graph.nodes[name].waits == ()


def test_producers_of_and_waits_of_helpers():
    graph = _build_prueba_acceso_graph()

    assert graph.producers_of("write_delta_merge") == ("group_by_fields",)
    assert graph.waits_of("parquet_data") == ("write_last_file",)
