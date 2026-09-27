"""
Cycle detection + global topological sort over a dataflow's *combined*
directed graph: data edges (`producer -> consumer`, from every `input`) and
wait edges (`waited_node -> waiting_node`, from every `waits` entry) are
walked as a single graph, never checked separately — a cycle can only
appear by combining both kinds of edge (an input whose `waits` points to a
node reachable from that same input via the normal `input` chain).
"""
from __future__ import annotations

from engine.graph import DataflowGraph


class GraphCycleError(Exception):
    """
    Raised when a dataflow's combined data+wait graph contains a cycle.

    `cycle` is the exact sequence of node names involved, in order, with
    the first name repeated at the end (e.g.
    `["write_last_file", "parquet_data", "group_by_fields", "write_last_file"]`)
    — never a generic "cycle detected" message.
    """

    def __init__(self, dataflow_name: str, cycle: list[str]):
        self.dataflow_name = dataflow_name
        self.cycle = cycle
        cycle_description = " -> ".join(cycle)
        super().__init__(f"dataflow '{dataflow_name}': cycle detected: {cycle_description}")


def _build_adjacency(graph: DataflowGraph) -> dict[str, list[str]]:
    adjacency: dict[str, list[str]] = {name: [] for name in graph.nodes}
    for producer, consumer in graph.data_edges:
        adjacency[producer].append(consumer)
    for waited_name, waiting_name in graph.wait_edges:
        adjacency[waited_name].append(waiting_name)
    return adjacency


def topological_sort(graph: DataflowGraph) -> list[str]:
    """
    Compute the global topological order of `graph`'s combined data+wait
    graph, raising `GraphCycleError` (naming every node in the cycle, in
    order) if one exists.

    Implementation: DFS with an explicit recursion stack, using three
    colors per node — WHITE (unvisited), GRAY (on the current DFS path),
    BLACK (fully processed). An edge into a GRAY node is a back edge, i.e.
    a cycle; when found, it is reconstructed from the current DFS path
    (from the GRAY node onward) rather than reported generically. The
    topological order is the reverse of the DFS postorder.
    """
    adjacency = _build_adjacency(graph)

    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {name: WHITE for name in graph.nodes}
    postorder: list[str] = []
    dfs_path: list[str] = []

    def visit(node_name: str) -> None:
        color[node_name] = GRAY
        dfs_path.append(node_name)

        for neighbor_name in adjacency[node_name]:
            if color[neighbor_name] == WHITE:
                visit(neighbor_name)
            elif color[neighbor_name] == GRAY:
                cycle_start = dfs_path.index(neighbor_name)
                cycle = dfs_path[cycle_start:] + [neighbor_name]
                raise GraphCycleError(graph.dataflow_name, cycle)
            # BLACK neighbor: already fully processed via another path, nothing to do.

        dfs_path.pop()
        color[node_name] = BLACK
        postorder.append(node_name)

    for node_name in graph.nodes:
        if color[node_name] == WHITE:
            visit(node_name)

    postorder.reverse()
    return postorder
