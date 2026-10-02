"""
Creation of the single SparkSession used by the whole program.

Configured for Delta Lake via `delta-spark`'s `configure_spark_with_delta_pip`
(which pulls in the matching delta-core jar for the installed pyspark
version), in path-based mode: no Hive metastore, no catalog-registered
tables. `type=table` outputs are resolved purely against physical paths
(`DeltaTable.forPath`, see `engine.writers`), never by a registered table
name.
"""
from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

DEFAULT_APP_NAME = "sdggroup_challenge"


def create_spark_session(app_name: str = DEFAULT_APP_NAME, with_delta: bool = True) -> SparkSession:
    """
    Build the single SparkSession for the whole program.

    Call this exactly once, before iterating over any dataflow, and pass
    the resulting session down (via `engine.executor.RunContext`) to every
    dataflow resolved from the metadata file — never create a new
    SparkSession per dataflow.

    `with_delta` (default True) wires in the Delta Lake extension/catalog
    and, via `configure_spark_with_delta_pip`, triggers Ivy to resolve the
    delta-spark/antlr jars on every call -- harmless once cached (no
    re-download, just a ~150ms dependency-resolution step), but pure
    overhead for a caller that only reads plain CSV/parquet and never
    touches a Delta table. `main.py` always needs it (the pipeline writes
    Delta tables), but `checks/diff_rows.py` and `checks/data_quality.py`
    only read raw CSVs, so they pass `with_delta=False` to skip it
    entirely and get a plain SparkSession.
    """
    builder = SparkSession.builder.appName(app_name)
    if not with_delta:
        return builder.getOrCreate()

    builder = builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    ).config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()
