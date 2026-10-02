"""
Human-readable description of a mapped dataflow graph: its nodes grouped
by kind (inputs / transformations / final-action outputs) and the exact
order the engine would execute them in.

Also renders, per node, the literal Spark instruction a real run would
build and execute for it (`spark.read...`, `df.filter(...)`,
`df.write...`, `DeltaTable...merge(...)`) -- rendered as a *string*, by
mirroring the exact call shape of engine.readers/engine.transformations/
engine.writers, never by actually calling them.

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

Keeping the rendered instructions in sync with the real dispatch code
(engine.readers.read_input / engine.transformations.apply_transformation /
engine.writers.write_output) is a manual invariant: if one of those
changes the Spark call it makes, the matching `_spark_call_*` function
below must be updated to match.
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


def _spark_call_input(node: FileInputNode, resolved_path: str) -> str:
    """Mirrors engine.readers.read_input's exact call shape."""
    call = f'spark.read.format("{node.config.format}")'
    if node.options:
        options = ", ".join(f'{key}="{value}"' for key, value in node.options.items())
        call += f".options({options})"
    call += f'.load("{resolved_path}")'
    return call


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
    lines.append(f"      spark           = {_spark_call_input(node, resolved_path)}")
    return lines


def _spark_call_transformation(node, input_name: str) -> str:
    """Mirrors engine.transformations.apply_transformation's exact call shape."""
    if isinstance(node, FilterTransformation):
        return f'{input_name}.filter("{node.config.filter}")'

    if isinstance(node, AddFieldsTransformation):
        call = input_name
        for field_spec in node.config.fields:
            call += f'.withColumn("{field_spec.name}", F.expr("{field_spec.expression}"))'
        return call

    if isinstance(node, GroupTransformation):
        group_by = ", ".join(f'"{field}"' for field in node.config.group_fields)
        aggregations = ", ".join(f'F.expr("{aggregation}")' for aggregation in node.config.aggregations)
        return f"{input_name}.groupBy({group_by}).agg({aggregations})"

    return "?"  # pragma: no cover - exhaustive over the discriminated union


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
        f"      spark             = {_spark_call_transformation(node, input_name)}",
    ]


def _spark_call_file_output(node: FileOutputNode, input_name: str, resolved_path: str) -> list[str]:
    """Mirrors engine.writers._write_file's exact call shape (one chained statement)."""
    call = f'{input_name}.write.format("{node.config.format}").mode("{node.config.save_mode}")'
    if node.config.partition:
        call += f'.partitionBy("{node.config.partition}")'
    call += f'.save("{resolved_path}")'
    return [call]


def _spark_call_table_output(node: TableOutputNode, input_name: str, table_path: str) -> list[str]:
    """
    Mirrors engine.writers._write_table's exact call shape.

    save_mode="append" is a single statement. save_mode="merge" is a real
    runtime branch in engine.writers (bootstrap overwrite on the very
    first run, `DeltaTable.merge()` afterwards) decided by
    `DeltaTable.isDeltaTable(spark, table_path)` -- a check against the
    actual filesystem/Delta log that `--dry-run` cannot perform without
    creating a SparkSession. Both branches are shown rather than guessed.
    """
    if node.config.save_mode == "append":
        return [f'{input_name}.write.format("delta").mode("append").save("{table_path}")']

    # save_mode == "merge" (the only other value TableOutputConfig allows).
    merge_condition = " AND ".join(f"target.`{key}` = source.`{key}`" for key in node.config.primary_key)
    return [
        f'if DeltaTable.isDeltaTable(spark, "{table_path}"):',
        f'    DeltaTable.forPath(spark, "{table_path}").alias("target") \\',
        f'        .merge({input_name}.alias("source"), "{merge_condition}") \\',
        f"        .whenMatchedUpdateAll() \\",
        f"        .whenNotMatchedInsertAll() \\",
        f"        .execute()",
        f"else:",
        f'    {input_name}.write.format("delta").mode("overwrite").save("{table_path}")',
        f"    # (bootstrap: no Delta table exists yet at this path)",
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
        lines.append("      spark      =")
        lines.extend(f"        {call_line}" for call_line in _spark_call_file_output(node, input_name, resolved_path))
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
        lines.append("      spark              =")
        lines.extend(f"        {call_line}" for call_line in _spark_call_table_output(node, input_name, table_path))
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
