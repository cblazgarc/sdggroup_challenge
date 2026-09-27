"""Dispatch of Spark writers for `outputs` nodes, by `type`/`config`."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from engine.metadata_schema import FileOutputNode, OutputNode, TableOutputNode


def write_output(spark: SparkSession, df: DataFrame, node: OutputNode, tables_base_path: str) -> None:
    """Write `df` for one output node (type=file or type=table)."""
    if isinstance(node, FileOutputNode):
        _write_file(df, node)
    elif isinstance(node, TableOutputNode):
        _write_table(spark, df, node, tables_base_path)
    else:
        raise ValueError(f"unsupported output node type: {node.type!r}")


def _write_file(df: DataFrame, node: FileOutputNode) -> None:
    """type=file: plain `df.write` with the node's `format`/`save_mode` (+ optional partition)."""
    writer = df.write.format(node.config.format).mode(node.config.save_mode)
    if node.config.partition:
        writer = writer.partitionBy(node.config.partition)
    writer.save(node.config.path)


def resolve_table_path(tables_base_path: str, table_name: str) -> str:
    """
    `config.table` (type=table outputs) carries no path in metadata.json —
    it is resolved against the engine's `tables_base_path` parameter
    (CLI `--tables-base-path`, never hardcoded here): `{tables_base_path}/{table_name}`.
    """
    return f"{tables_base_path.rstrip('/')}/{table_name}"


def _write_table(spark: SparkSession, df: DataFrame, node: TableOutputNode, tables_base_path: str) -> None:
    """
    type=table: always Delta Lake, path-based (no Hive metastore, no
    registered catalog table — `DeltaTable.forPath` only).

    save_mode="append": plain `df.write.format("delta").mode("append")`.
    save_mode="merge": `DeltaTable.merge()` keyed on `config.primary_key`
    (`whenMatchedUpdateAll` + `whenNotMatchedInsertAll`) — except on the very
    first run, when no Delta table exists yet at the target path, in which
    case there is nothing to merge into and the table is bootstrapped with
    a plain overwrite write instead.
    """
    from delta.tables import DeltaTable  # local import: only needed for type=table outputs

    table_path = resolve_table_path(tables_base_path, node.config.table)

    if node.config.save_mode == "append":
        df.write.format("delta").mode("append").save(table_path)
        return

    # The only other save_mode TableOutputConfig allows is "merge".
    if DeltaTable.isDeltaTable(spark, table_path):
        delta_table = DeltaTable.forPath(spark, table_path)
        merge_condition = " AND ".join(
            f"target.`{key}` = source.`{key}`" for key in node.config.primary_key
        )
        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(table_path)
