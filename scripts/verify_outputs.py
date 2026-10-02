"""
Evidence of the outputs' behavior (overwrite / append / merge) after each
`main.py` run -- point 4 of the challenge statement.

Not part of the metadata-driven engine nor of the mandatory checks from
point 5 (those live in checks/ and operate on the raw CSVs). This script is
a verification/evidence utility: it reads the 4 destinations of the
"prueba-acceso" dataflow exactly as they were left on disk/Delta after a
run, and prints what's needed to demonstrate that each `save_mode` behaved
as expected.

Typical usage (see README):
    python main.py --metadata metadata.json --year 2024
    python scripts/verify_outputs.py > docs/evidence/after_2024.log

    python main.py --metadata metadata.json --year 2025
    python scripts/verify_outputs.py > docs/evidence/after_2025.log

Accepts the same path arguments as `main.py` to avoid hardcoding anything:
--metadata (to automatically resolve table names/paths of the
"prueba-acceso" dataflow) and --tables-base-path. Does not require --year:
it doesn't run the pipeline, it only reads what was already written.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allows running this script as `python scripts/verify_outputs.py` from the
# project root: without this, Python adds `scripts/` (not the root) to
# sys.path and `import engine...` fails with ModuleNotFoundError. Same
# pattern conftest.py uses for the tests.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.cli import DEFAULT_TABLES_BASE_PATH
from engine.paths import resolve_project_path
from engine.spark_session import create_spark_session
from engine.writers import resolve_table_path

LAST_FILE_PATH = "/data/demo/output/last"
HISTORIC_FILE_PATH = "/data/demo/output/historic"
MERGE_TABLE_NAME = "demo"
APPEND_TABLE_NAME = "raw_demo"


def _print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def _verify_last_file(spark, tables_base_path: str) -> None:
    _print_header("write_last_file (type=file, save_mode=overwrite)")
    path = resolve_project_path(LAST_FILE_PATH)
    try:
        df = spark.read.parquet(path)
    except Exception as exc:  # noqa: BLE001 - this is a read-only diagnostic script
        print(f"Could not read '{path}': {exc}")
        return
    print(f"Path: {path}")
    print(f"Current rows: {df.count()}")
    print("If 'overwrite' works correctly, this number is ONLY the current")
    print("run's (filtered), never the sum of previous runs.")
    df.show(10, truncate=False)


def _verify_historic_file(spark) -> None:
    _print_header("write_historic_file (type=file, save_mode=append, partition=load_date)")
    path = resolve_project_path(HISTORIC_FILE_PATH)
    try:
        df = spark.read.parquet(path)
    except Exception as exc:  # noqa: BLE001
        print(f"Could not read '{path}': {exc}")
        return
    print(f"Path: {path}")
    print(f"Total accumulated rows: {df.count()}")
    print("Partitions (load_date) present:")
    df.select("load_date").distinct().orderBy("load_date").show(truncate=False)
    print("With 'append' the total grows across runs and earlier partitions")
    print("are left untouched (two runs on the same day land in the same")
    print("load_date partition -- a known limitation, documented in the PPT).")


def _verify_delta_table(spark, tables_base_path: str, table_name: str, label: str) -> None:
    _print_header(f"{label} (type=table, Delta, table={table_name!r})")
    from delta.tables import DeltaTable

    table_path = resolve_table_path(tables_base_path, table_name)
    print(f"Path: {table_path}")
    if not DeltaTable.isDeltaTable(spark, table_path):
        print("Does not exist yet as a Delta table at this path (not run yet).")
        return

    delta_table = DeltaTable.forPath(spark, table_path)
    current_df = delta_table.toDF()
    print(f"Current rows: {current_df.count()}")

    print("\nOperation history (most recent to oldest):")
    history_df = delta_table.history().select(
        "version", "timestamp", "operation", "operationMetrics"
    )
    history_df.show(truncate=False)

    print("Reading 'operationMetrics' by version:")
    for row in history_df.orderBy("version").collect():
        metrics = row["operationMetrics"] or {}
        if row["operation"] == "MERGE":
            print(
                f"  v{row['version']} MERGE -> "
                f"inserted={metrics.get('numTargetRowsInserted', '?')}, "
                f"updated={metrics.get('numTargetRowsUpdated', '?')}, "
                f"deleted={metrics.get('numTargetRowsDeleted', '?')}"
            )
        elif row["operation"] == "WRITE":
            print(
                f"  v{row['version']} WRITE (mode={metrics.get('mode', '?')}) -> "
                f"rows_written={metrics.get('numOutputRows', '?')}"
            )
        else:
            print(f"  v{row['version']} {row['operation']} -> {metrics}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Reads the 4 destinations of the 'prueba-acceso' dataflow and shows "
        "evidence of overwrite/append/merge from what's already on disk."
    )
    parser.add_argument(
        "--tables-base-path",
        default=DEFAULT_TABLES_BASE_PATH,
        help="Must match the one used when running main.py (default: %(default)s)",
    )
    args = parser.parse_args(argv)

    spark = create_spark_session()
    try:
        _verify_last_file(spark, args.tables_base_path)
        _verify_historic_file(spark)
        _verify_delta_table(spark, args.tables_base_path, MERGE_TABLE_NAME, "write_delta_merge")
        _verify_delta_table(spark, args.tables_base_path, APPEND_TABLE_NAME, "write_delta_raw")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
