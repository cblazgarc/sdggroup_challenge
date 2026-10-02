"""
Check (b) from point 5 of the challenge statement: data-quality problems in
a population file -- specifically, that 'Ambos sexos' == 'Hombres' +
'Mujeres' for every municipio.

Standalone, completely separate from the metadata-driven engine (point 1),
reusing the same dependency as the engine (Spark) instead of a new
library, exactly like checks/diff_rows.py. Operates on a raw population
CSV (not on the pipeline's outputs), and is generic over *which* input
file: --input accepts any poblacionXXXX.csv, defaulting to the 2025 file
since that's the one the challenge statement names for this check, but
nothing here is hardcoded to 2025 -- running it with --input pointing at
the 2024 file works identically.

Granularity: one row per (provincia, municipio) -- the same key level
diff_rows.py uses minus `sexo`, since this check is precisely about
comparing the three `sexo` values against each other within that key.
This includes the provincial aggregate rows (municipio="N/A"): if the
three `sexo` categories are present there too, the same rule must hold.

Reporting format (decided here, since the statement doesn't specify one):
- A row is flagged when either (a) 'Ambos sexos' != 'Hombres' + 'Mujeres'
  (issue="sum_mismatch"), or (b) any of the three `sexo` categories is
  missing entirely for that municipio (issue="missing_category") -- a
  municipio that silently lacks a category is also a data-quality problem,
  not something to skip silently.
- Only flagged rows are written to the detail output (not a full dump of
  every municipio), same convention as diff_rows.py's detail CSV.
- The process exits with a non-zero code when issues are found (see
  EXIT_QUALITY_ISSUES below), so it can be used as a pass/fail check (e.g.
  from a test or a CI step), not just an informational report.
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
DEFAULT_INPUT_PATH = PROJECT_ROOT / "assets" / "poblacion2025.csv"
DEFAULT_OUTPUT_PATH = Path(__file__).resolve().parent / "output" / "data_quality_issues.csv"

EXIT_OK = 0
EXIT_QUALITY_ISSUES = 20

SEXO_CATEGORIES = ["Hombres", "Mujeres", "Ambos sexos"]
KEY_COLUMNS = ["provincia", "municipio"]


def load_population_df(spark: SparkSession, path: str) -> DataFrame:
    """
    Read a poblacionXXXX.csv file (provincia;municipio;sexo;total) and cast
    `total` to integer. The CSV uses "." as a thousands separator, not a
    decimal point (e.g. "1.932" = 1932), hence the `regexp_replace` before
    the cast. Spark already strips the file's leading UTF-8 BOM natively.
    """
    df = spark.read.format("csv").option("header", "true").option("delimiter", ";").load(path)
    return df.withColumn("total", F.regexp_replace(F.col("total"), r"\.", "").cast("int"))


def compute_quality_issues(df: DataFrame) -> DataFrame:
    """
    Pivot `sexo` into columns per (provincia, municipio) and flag the rows
    where 'Ambos sexos' doesn't equal 'Hombres' + 'Mujeres', or where any
    of the three categories is missing for that municipio.
    """
    pivoted = (
        df.groupBy(*KEY_COLUMNS)
        .pivot("sexo", SEXO_CATEGORIES)
        .agg(F.first("total"))
        .withColumnRenamed("Hombres", "hombres")
        .withColumnRenamed("Mujeres", "mujeres")
        .withColumnRenamed("Ambos sexos", "ambos_sexos")
    )
    pivoted = pivoted.withColumn("hombres_mas_mujeres", F.col("hombres") + F.col("mujeres"))
    pivoted = pivoted.withColumn(
        "issue",
        F.when(
            F.col("hombres").isNull() | F.col("mujeres").isNull() | F.col("ambos_sexos").isNull(),
            F.lit("missing_category"),
        )
        .when(F.col("ambos_sexos") != F.col("hombres_mas_mujeres"), F.lit("sum_mismatch"))
        .otherwise(F.lit(None)),
    )
    issues = pivoted.filter(F.col("issue").isNotNull())
    issues = issues.withColumn("diff", F.col("ambos_sexos") - F.col("hombres_mas_mujeres"))
    return issues.select(
        *KEY_COLUMNS, "hombres", "mujeres", "hombres_mas_mujeres", "ambos_sexos", "diff", "issue"
    ).orderBy(*KEY_COLUMNS)


def summarize(municipio_count: int, issues_df: DataFrame) -> str:
    issue_counts = {
        row["issue"]: row["count"] for row in issues_df.groupBy("issue").count().collect()
    }
    sum_mismatch = issue_counts.get("sum_mismatch", 0)
    missing_category = issue_counts.get("missing_category", 0)
    total_issues = sum_mismatch + missing_category
    lines = [
        f"Municipios/key rows checked (provincia+municipio): {municipio_count}",
        f"Violations of 'Ambos sexos' != 'Hombres' + 'Mujeres': {sum_mismatch}",
        f"Sexo categories missing for some municipio: {missing_category}",
        f"Total issues detected: {total_issues}",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check (b): data quality -- 'Ambos sexos' == 'Hombres' + 'Mujeres'"
    )
    parser.add_argument("--input", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
    args = parser.parse_args(argv)

    spark = create_spark_session(app_name="sdggroup_challenge-data_quality", with_delta=False)
    try:
        df = load_population_df(spark, args.input)
        municipio_count = df.select(*KEY_COLUMNS).distinct().count()
        issues_df = compute_quality_issues(df).cache()
        issue_count = issues_df.count()

        print(summarize(municipio_count, issues_df))

        if issue_count > 0:
            issues_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(args.output)
            print(f"\nFull detail ({issue_count} rows with issues) written to: {args.output}")
            return EXIT_QUALITY_ISSUES

        print("\nNo violations detected.")
        return EXIT_OK
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
