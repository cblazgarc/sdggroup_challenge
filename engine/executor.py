"""
Resolves and executes one dataflow's DAG against a single, shared
SparkSession.

For each dataflow:
  1. Validate it and compute its global topological order (cycle detection
     over the combined data+wait graph — see `engine.topology`).
  2. Find its leaf nodes: the `outputs` that are never a data-edge producer
     for another node (i.e. never referenced via someone else's `input`).
  3. Fire those leaves in the order they appear in the topological order —
     never in declaration order or any other arbitrary order — because a
     `waits` edge crossing branches means one branch's output must finish
     before another branch's input can be read.
  4. Resolve each branch recursively, from the output down to its root
     input(s), memoizing every intermediate DataFrame so a node that feeds
     more than one output is read/transformed only once; that DataFrame is
     `.cache()`d right before it fans out, matching Spark's lazy semantics.
  5. Resolving an input node forces (`waits`) the referenced node(s) to
     completion — an actual write action for an output, not just a lazy
     DataFrame — before that input is read.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from engine.graph import DataflowGraph, NodeKind
from engine.readers import read_input
from engine.templating import build_execution_context
from engine.topology import topological_sort
from engine.transformations import apply_transformation
from engine.writers import write_output


@dataclass(frozen=True)
class RunContext:
    """Everything a dataflow resolution needs beyond the graph itself."""

    spark: SparkSession
    year: int
    tables_base_path: str

    @property
    def template_context(self) -> dict[str, Any]:
        return build_execution_context(self.year)


class DataflowResolver:
    """Resolves and executes a single `DataflowGraph` (see module docstring)."""

    def __init__(self, graph: DataflowGraph, run_context: RunContext):
        self._graph = graph
        self._run_context = run_context
        # Memoized lazy DataFrames for input/transformation nodes.
        self._dataframe_cache: dict[str, DataFrame] = {}
        # Output nodes whose write action has already run (idempotent re-entry).
        self._executed_outputs: set[str] = set()
        # Immediate data-edge out-degree per node: >1 means it feeds more
        # than one consumer and must be cached before branching.
        self._fanout: Counter[str] = Counter(producer for producer, _ in graph.data_edges)

    def run(self) -> list[str]:
        """
        Validate the dataflow, execute every output, and return the global
        topological order that was computed (and used to sequence them).
        """
        topo_order = topological_sort(self._graph)

        leaf_outputs = {
            name
            for name, node in self._graph.nodes.items()
            if node.kind == NodeKind.OUTPUT and self._fanout[name] == 0
        }
        ordered_leaves = [name for name in topo_order if name in leaf_outputs]

        for output_name in ordered_leaves:
            self._resolve_output(output_name)

        return topo_order

    def _should_cache(self, node_name: str) -> bool:
        return self._fanout[node_name] > 1

    def _resolve_node(self, node_name: str) -> DataFrame:
        """Resolve any data-producing node (input or transformation) to its DataFrame."""
        graph_node = self._graph.nodes[node_name]
        if graph_node.kind == NodeKind.INPUT:
            return self._resolve_input(node_name)
        if graph_node.kind == NodeKind.TRANSFORMATION:
            return self._resolve_transformation(node_name)
        raise ValueError(
            f"'{node_name}' is an output node and cannot be used as a data source for another node"
        )

    def _resolve_input(self, node_name: str) -> DataFrame:
        if node_name in self._dataframe_cache:
            return self._dataframe_cache[node_name]

        graph_node = self._graph.nodes[node_name]

        # `waits`: force each referenced node to completion before reading.
        for waited_name in graph_node.waits:
            self._force_wait(waited_name)

        df = read_input(self._run_context.spark, graph_node.node, self._run_context.template_context)
        if self._should_cache(node_name):
            df = df.cache()

        self._dataframe_cache[node_name] = df
        return df

    def _resolve_transformation(self, node_name: str) -> DataFrame:
        if node_name in self._dataframe_cache:
            return self._dataframe_cache[node_name]

        graph_node = self._graph.nodes[node_name]
        (producer_name,) = graph_node.data_inputs
        upstream_df = self._resolve_node(producer_name)

        df = apply_transformation(upstream_df, graph_node.node)
        if self._should_cache(node_name):
            df = df.cache()

        self._dataframe_cache[node_name] = df
        return df

    def _resolve_output(self, node_name: str) -> None:
        if node_name in self._executed_outputs:
            return

        graph_node = self._graph.nodes[node_name]
        (producer_name,) = graph_node.data_inputs
        upstream_df = self._resolve_node(producer_name)

        write_output(self._run_context.spark, upstream_df, graph_node.node, self._run_context.tables_base_path)
        self._executed_outputs.add(node_name)

    def _force_wait(self, waited_name: str) -> None:
        """
        Force `waited_name` to completion before the input node that
        declared the `waits` is resolved.

        Only an output node has a persistent side effect worth forcing, so
        that is the meaningful case (it triggers `_resolve_output`, i.e. an
        actual write action). `waits` referencing an input/transformation
        name is accepted (the schema's referential-integrity check does not
        restrict it), but there is no action to force there beyond building
        its (lazy, possibly cached) DataFrame.
        """
        waited_node = self._graph.nodes[waited_name]
        if waited_node.kind == NodeKind.OUTPUT:
            self._resolve_output(waited_name)
        else:
            self._resolve_node(waited_name)


def resolve_dataflow(graph: DataflowGraph, run_context: RunContext) -> list[str]:
    """Validate and execute one dataflow. Returns its global topological order."""
    return DataflowResolver(graph, run_context).run()


def resolve_all(graphs: dict[str, DataflowGraph], run_context: RunContext) -> dict[str, list[str]]:
    """
    Validate and execute every dataflow, reusing the same `RunContext`
    (and therefore the same SparkSession) for all of them.
    """
    return {name: resolve_dataflow(graph, run_context) for name, graph in graphs.items()}
