"""Dispatch of Spark transformations for `transformations` nodes, by `type`/`config`."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from engine.metadata_schema import (
    AddFieldsTransformation,
    FilterTransformation,
    GroupTransformation,
    TransformationNode,
)


def apply_transformation(df: DataFrame, node: TransformationNode) -> DataFrame:
    """
    Apply one transformation node to `df`, chaining the Spark operation that
    matches its `type`:
      - filter:     `df.filter(config.filter)`
      - add_fields: `df.withColumn(name, expr(expression))` per declared field
      - group:      `df.groupBy(*group_fields).agg(*aggregations)`, each
                     aggregation being a raw SQL expression (e.g.
                     "sum(total) as total_ambos_sexos"), so alias handling
                     comes from Spark's own `expr()` parsing.
    """
    if isinstance(node, FilterTransformation):
        return df.filter(node.config.filter)

    if isinstance(node, AddFieldsTransformation):
        for field_spec in node.config.fields:
            df = df.withColumn(field_spec.name, F.expr(field_spec.expression))
        return df

    if isinstance(node, GroupTransformation):
        aggregations = [F.expr(aggregation) for aggregation in node.config.aggregations]
        return df.groupBy(*node.config.group_fields).agg(*aggregations)

    raise ValueError(f"unsupported transformation node type: {node.type!r}")
