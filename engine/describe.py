"""
Human-readable description of a mapped dataflow graph: its nodes grouped
by kind (inputs / transformations / final-action outputs) and the exact
order the engine would execute them in.

Used by `--dry-run` (see main.py): everything here is derived purely from
the already-parsed metadata/graph plus string-level path resolution
(engine.paths / engine.templating) -- no SparkSession is created and no
file, table or directory is touched. This module must stay Spark-free so
`--dry-run` never pays Spark's startup cost (or Delta's Ivy resolution)
just to print a plan -- which is also why it does NOT import
`engine.writers.resolve_table_path` even though the logic is identical:
`engine/writers.py` imports `pyspark.sql` at module level, so importing
it would make `--dry-run` require pyspark to be installed just to print
a plan. `_resolve_table_path` below duplicates that one-line string
computation to avoid the transitive pyspark dependency.
"""
from __future__ import annotations

from typing import Any

from engine.graph import DataflowGraph, GraphNode, NodeKind
from engine.metadata_schema import (
    AddFieldsTransformation,
    FileInputNode,
    FileOutputNode,
    FilterTransformation,
    GroupTransformation,
    TableOutputNode,
)
from engine.paths import resolve_project_path
from engine.templating import resolve_template


def _resolve_table_path(tables_base_path: str, table_name: str) -> str:
    """
    Pure-string duplicate of `engine.writers.resolve_table_path`, kept in
    sync with it deliberately (see module docstring for why it isn't
    imported from there): `{tables_base_path}/{table_name}`, rebased
    under the project root the same way as every other metadata.json-style
    path.
    """
    resolved_base_path = resolve_project_path(tables_base_path)
    return f"{resolved_base_path.rstrip('/')}/{table_name}"


def _describe_input(node: FileInputNode, waits: tuple[str, ...], template_context: dict[str, Any]) -> list[str]:
    templated_path = resolve_template(node.config.path, template_context)
    resolved_path = resolve_project_path(templated_path)

    lines = [f"  - {node.name} [input/file]"]
    if templated_path != node.config.path:
        lines.append(f"      path (template) = {node.config.path}")
        lines.append(f"      path (resolved) = {resolved_path}")
    else:
        lines.append(f"      path            = {resolved_path}")
    lines.append(f"      format          = {node.config.format}")
    if node.options:
        lines.append(f"      options         = {node.options}")
    if waits:
        lines.append(f"      waits for       = {', '.join(waits)}  (must finish writing before this is read)")
    return lines


def _describe_transformation(node, data_inputs: tuple[str, ...]) -> list[str]:
    input_name = data_inputs[0] if data_inputs else "?"

    if isinstance(node, FilterTransformation):
        detail = f'filter expression = "{node.config.filter}"'
    elif isinstance(node, AddFieldsTransformation):
        fields = ", ".join(f"{f.name} = {f.expression}" for f in node.config.fields)
        detail = f"add fields        = {fields}"
    elif isinstance(node, GroupTransformation):
        detail = (
            f"group by          = {node.config.group_fields}\n"
            f"      aggregations      = {node.config.aggregations}"
        )
    else:  # pragma: no cover - exhaustive over the discriminated union
        detail = "?"

    return [
        f"  - {node.name} [transformation/{node.type}] input={input_name}",
        f"      {detail}",
    ]


def _describe_output(node, data_inputs: tuple[str, ...], tables_base_path: str) -> list[str]:
    input_name = data_inputs[0] if data_inputs else "?"

    if isinstance(node, FileOutputNode):
        resolved_path = resolve_project_path(node.config.path)
        lines = [
            f"  - {node.name} [output/file] input={input_name}  <-- final action",
            f"      path       = {resolved_path}",
            f"      format     = {node.config.format}",
            f"      save_mode  = {node.config.save_mode}",
        ]
        if node.config.partition:
            lines.append(f"      partition  = {node.config.partition}")
        return lines

    if isinstance(node, TableOutputNode):
        table_path = _resolve_table_path(tables_base_path, node.config.table)
        lines = [
            f"  - {node.name} [output/table] input={input_name}  <-- final action",
            f"      table (Delta path) = {table_path}",
            f"      save_mode          = {node.config.save_mode}",
        ]
        if node.config.primary_key:
            lines.append(f"      primary_key        = {node.config.primary_key}")
        return lines

    return [f"  - {node.name} [output/?]"]  # pragma: no cover - exhaustive over the discriminated union


def _describe_node(graph_node: GraphNode, template_context: dict[str, Any], tables_base_path: str) -> list[str]:
    if graph_node.kind == NodeKind.INPUT:
        return _describe_input(graph_node.node, graph_node.waits, template_context)
    if graph_node.kind == NodeKind.TRANSFORMATION:
        return _describe_transformation(graph_node.node, graph_node.data_inputs)
    return _describe_output(graph_node.node, graph_node.data_inputs, tables_base_path)


def describe_dataflow(
    graph: DataflowGraph,
    topo_order: list[str],
    template_context: dict[str, Any],
    tables_base_path: str,
) -> str:
    """
    Render a report of one dataflow's mapped graph for `--dry-run`:
    every node grouped by kind, plus the exact execution order the
    engine would follow (same `topo_order` a real run uses).
    """
    lines: list[str] = [f"Dataflow '{graph.dataflow_name}' -- {len(graph.nodes)} nodes mapped"]

    lines.append("")
    lines.append("Inputs:")
    input_names = [n for n in topo_order if graph.nodes[n].kind == NodeKind.INPUT]
    for name in input_names:
        lines.extend(_describe_node(graph.nodes[name], template_context, tables_base_path))

    lines.append("")
    lines.append("Transformations:")
    transformation_names = [n for n in topo_order if graph.nodes[n].kind == NodeKind.TRANSFORMATION]
    if not transformation_names:
        lines.append("  (none)")
    for name in transformation_names:
        lines.extend(_describe_node(graph.nodes[name], template_context, tables_base_path))

    lines.append("")
    lines.append("Outputs (final actions):")
    output_names = [n for n in topo_order if graph.nodes[n].kind == NodeKind.OUTPUT]
    for name in output_names:
        lines.extend(_describe_node(graph.nodes[name], template_context, tables_base_path))

    lines.append("")
    lines.append(f"Execution order (topological): {' -> '.join(topo_order)}")

    return "\n".join(lines)
