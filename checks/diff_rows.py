"""
Check (a) from point 5 of the challenge statement: which rows changed
between the 2024 and 2025 population files.

Standalone, completely separate from the metadata-driven engine (point 1),
but reuses the same dependency as the engine (Spark) instead of introducing
a new library -- just like `engine.readers` reads with
`spark.read.format("csv")`, this does the same over the two original CSVs
(the same file read by the `demo_data` node in metadata.json), never over
the pipeline's outputs (`data/demo/output/...`). That's why the result is
identical no matter the order, how many times, or with which --year
`main.py` has been run.

Diff granularity: one row = one (provincia, municipio, sexo) combination,
the file's natural key (appears exactly once per year, including the
provincial aggregate rows with municipio="N/A" and the "Ambos sexos" row
for each municipio -- unlike the pipeline, nothing is filtered out here).
A row "changes" when its `total` differs between 2024 and 2025; additions
(key new in 2025, status="added") and removals (key that disappears,
status="removed") are also reported. Rows identical in both years are not
listed in the detail output.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.spark_session import create_spark_session  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_2024_PATH = PROJECT_ROOT / "assets" / "poblacion2024.csv"
DEFAULT_2025_PATH = PROJECT_ROOT / "assets" / "poblacion2025.csv"
DEFAULT_OUTPUT_PATH = Path(__file__).resolve().parent / "output" / "diff_2024_2025.csv"

KEY_COLUMNS = ["provincia", "municipio", "sexo"]


def load_population_df(spark: SparkSession, path: str) -> DataFrame:
    """
    Read a poblacionXXXX.csv file (provincia;municipio;sexo;total) and cast
    `total` to integer. The CSV uses "." as a thousands separator, not a
    decimal point (e.g. "1.932" = 1932), hence the `regexp_replace` before
    the cast -- casting straight to int/double without it would misparse
    the number. Spark already strips the file's leading UTF-8 BOM natively.
    """
    df = spark.read.format("csv").option("header", "true").option("delimiter", ";").load(path)
    return df.withColumn("total", F.regexp_replace(F.col("total"), r"\.", "").cast("int"))


def compute_diff(old_df: DataFrame, new_df: DataFrame) -> DataFrame:
    """
    Full outer join on (provincia, municipio, sexo) between the old and new
    snapshots, keeping only the rows that differ: "changed" (same key,
    different total), "added" (key only in `new_df`), or "removed" (key
    only in `old_df`).
    """
    old = old_df.select(*KEY_COLUMNS, F.col("total").alias("total_2024"))
    new = new_df.select(*KEY_COLUMNS, F.col("total").alias("total_2025"))

    joined = old.join(new, on=KEY_COLUMNS, how="full_outer")
    joined = joined.withColumn(
        "status",
        F.when(F.col("total_2024").isNull(), F.lit("added"))
        .when(F.col("total_2025").isNull(), F.lit("removed"))
        .when(F.col("total_2024") != F.col("total_2025"), F.lit("changed"))
        .otherwise(F.lit("unchanged")),
    )
    diffs = joined.filter(F.col("status") != F.lit("unchanged"))
    diffs = diffs.withColumn("delta", F.col("total_2025") - F.col("total_2024"))
    return diffs.select(*KEY_COLUMNS, "total_2024", "total_2025", "delta", "status").orderBy(
        *KEY_COLUMNS
    )


def summarize(old_df: DataFrame, new_df: DataFrame, diffs_df: DataFrame) -> str:
    old_count = old_df.count()
    new_count = new_df.count()
    status_counts = {row["status"]: row["count"] for row in diffs_df.groupBy("status").count().collect()}
    changed = status_counts.get("changed", 0)
    added = status_counts.get("added", 0)
    removed = status_counts.get("removed", 0)
    total_old = old_df.agg(F.sum("total")).first()[0] or 0
    total_new = new_df.agg(F.sum("total")).first()[0] or 0
    lines = [
        f"Filas en 2024: {old_count}",
        f"Filas en 2025: {new_count}",
        f"Filas sin cambios: {old_count - changed - removed}",
        f"Filas cambiadas (mismo municipio+sexo, total distinto): {changed}",
        f"Filas nuevas en 2025: {added}",
        f"Filas eliminadas respecto a 2024: {removed}",
        f"Poblacion total 2024: {total_old}",
        f"Poblacion total 2025: {total_new}",
        f"Variacion neta: {total_new - total_old:+d}",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Comprobacion (a): filas que cambian de poblacion2024.csv a poblacion2025.csv"
    )
    parser.add_argument("--input-2024", default=str(DEFAULT_2024_PATH))
    parser.add_argument("--input-2025", default=str(DEFAULT_2025_PATH))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
    args = parser.parse_args(argv)

    spark = create_spark_session(app_name="sdggroup_challenge-diff_rows")
    try:
        old_df = load_population_df(spark, args.input_2024)
        new_df = load_population_df(spark, args.input_2025)
        diffs_df = compute_diff(old_df, new_df).cache()

        print(summarize(old_df, new_df, diffs_df))
        diffs_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(args.output)
        print(f"\nDetalle completo ({diffs_df.count()} filas) escrito en: {args.output}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
