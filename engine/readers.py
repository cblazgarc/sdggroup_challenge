"""Dispatch of Spark readers for `inputs` nodes, by `type`/`config`."""
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from engine.metadata_schema import FileInputNode
from engine.templating import resolve_template


def read_input(spark: SparkSession, input_node: FileInputNode, template_context: dict[str, Any]) -> DataFrame:
    """
    Build the DataFrame for one `inputs` node.

    The only supported input `type` today is "file": `config.format` is
    passed straight to `DataFrameReader.format(...)`, so any format Spark
    understands (csv, parquet, json, ...) works without a dedicated branch
    here. `config.path` may contain `{{ variable }}` templating (e.g.
    `{{ year }}`), resolved against `template_context` before reading. Any
    `options` declared on the node (e.g. csv `header`/`delimiter`) are
    passed through to the reader as-is.
    """
    resolved_path = resolve_template(input_node.config.path, template_context)

    reader = spark.read.format(input_node.config.format)
    if input_node.options:
        reader = reader.options(**input_node.options)

    return reader.load(resolved_path)
