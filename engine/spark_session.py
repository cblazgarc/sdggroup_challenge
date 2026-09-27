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


def create_spark_session(app_name: str = DEFAULT_APP_NAME) -> SparkSession:
    """
    Build the single SparkSession for the whole program.

    Call this exactly once, before iterating over any dataflow, and pass
    the resulting session down (via `engine.executor.RunContext`) to every
    dataflow resolved from the metadata file — never create a new
    SparkSession per dataflow.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
