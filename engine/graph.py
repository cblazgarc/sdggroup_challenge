"""
In-memory DAG structure built from an already-validated :class:`~engine.metadata_schema.Dataflow`.

Two distinct kinds of edges are tracked, on purpose, so a later execution
resolver can tell them apart:

  - **data edges** (``producer -> consumer``): one per ``input`` field on a
    transformation or output node. Resolving a consumer requires the
    producer's output to have already been computed.
  - **wait edges** (``waited_node -> waiting_node``): one per entry in an
    input node's ``waits`` list. These carry no data — they only force
    ``waited_node`` to run to completion *before* the input node that
    declares the wait is resolved (read). ``waits`` is modelled as an
    action attached to the input node itself, never as a node or a data
    edge of its own.

No cycle detection or topological sort is performed here — that, along with
the SparkSession and actual execution, is left to a later step.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from engine.metadata_schema import Dataflow, FileInputNode, MetadataFile, OutputNode, TransformationNode


class NodeKind(str, Enum):
    INPUT = "input"
    TRANSFORMATION = "transformation"
    OUTPUT = "output"


@dataclass(frozen=True)
class GraphNode:
    """
    A single node in the graph, wrapping its validated metadata model.

    ``data_inputs`` holds the producer name(s) this node reads from (derived
    from its ``input`` field; empty for input nodes, which read from an
    external source instead).

    ``waits`` holds order-only dependencies and is only ever non-empty for
    ``NodeKind.INPUT`` nodes — it is kept separate from ``data_inputs`` so
    callers never have to guess which kind of edge they are looking at.
    """

    name: str
    kind: NodeKind
    node: FileInputNode | TransformationNode | OutputNode
    data_inputs: tuple[str, ...] = ()
    waits: tuple[str, ...] = ()


@dataclass
class DataflowGraph:
    """
    The DAG-shaped structure for one dataflow.

    ``data_edges`` and ``wait_edges`` are kept as separate edge lists
    (rather than a single mixed graph) precisely so a topological sort
    implemented later can combine them explicitly instead of accidentally
    treating one kind as the other.
    """

    dataflow_name: str
    nodes: dict[str, GraphNode] = field(default_factory=dict)
    data_edges: list[tuple[str, str]] = field(default_factory=list)
    wait_edges: list[tuple[str, str]] = field(default_factory=list)

    def producers_of(self, name: str) -> tuple[str, ...]:
        """Names of the node(s) that feed data into ``name`` via `input`."""
        return self.nodes[name].data_inputs

    def waits_of(self, name: str) -> tuple[str, ...]:
        """Names of the node(s) that must run to completion before ``name`` (an input node) is resolved."""
        return self.nodes[name].waits


def build_graph(dataflow: Dataflow) -> DataflowGraph:
    """Build the in-memory DAG structure for one already-validated Dataflow."""
    graph = DataflowGraph(dataflow_name=dataflow.name)

    for input_node in dataflow.inputs:
        graph.nodes[input_node.name] = GraphNode(
            name=input_node.name,
            kind=NodeKind.INPUT,
            node=input_node,
            data_inputs=(),
            waits=tuple(input_node.waits),
        )

    for transformation in dataflow.transformations:
        graph.nodes[transformation.name] = GraphNode(
            name=transformation.name,
            kind=NodeKind.TRANSFORMATION,
            node=transformation,
            data_inputs=(transformation.input,),
        )

    for output in dataflow.outputs:
        graph.nodes[output.name] = GraphNode(
            name=output.name,
            kind=NodeKind.OUTPUT,
            node=output,
            data_inputs=(output.input,),
        )

    for graph_node in graph.nodes.values():
        for producer_name in graph_node.data_inputs:
            graph.data_edges.append((producer_name, graph_node.name))
        for waited_name in graph_node.waits:
            graph.wait_edges.append((waited_name, graph_node.name))

    return graph


def build_graphs(metadata: MetadataFile) -> dict[str, DataflowGraph]:
    """Build one :class:`DataflowGraph` per dataflow declared in a validated MetadataFile."""
    return {dataflow.name: build_graph(dataflow) for dataflow in metadata.dataflows}
