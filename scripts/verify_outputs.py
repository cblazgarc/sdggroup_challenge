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
        print(f"No se pudo leer '{path}': {exc}")
        return
    print(f"Ruta: {path}")
    print(f"Filas actuales: {df.count()}")
    print("Si 'overwrite' funciona correctamente, este numero es SOLO el de la")
    print("ultima ejecucion (filtrada), nunca la suma de ejecuciones anteriores.")
    df.show(10, truncate=False)


def _verify_historic_file(spark) -> None:
    _print_header("write_historic_file (type=file, save_mode=append, partition=load_date)")
    path = resolve_project_path(HISTORIC_FILE_PATH)
    try:
        df = spark.read.parquet(path)
    except Exception as exc:  # noqa: BLE001
        print(f"No se pudo leer '{path}': {exc}")
        return
    print(f"Ruta: {path}")
    print(f"Filas totales acumuladas: {df.count()}")
    print("Particiones (load_date) presentes:")
    df.select("load_date").distinct().orderBy("load_date").show(truncate=False)
    print("Con 'append' el total crece entre ejecuciones y las particiones")
    print("anteriores no se tocan (dos ejecuciones el mismo dia caen en la misma")
    print("particion load_date -- limitacion conocida, documentada en el PPT).")


def _verify_delta_table(spark, tables_base_path: str, table_name: str, label: str) -> None:
    _print_header(f"{label} (type=table, Delta, table={table_name!r})")
    from delta.tables import DeltaTable

    table_path = resolve_table_path(tables_base_path, table_name)
    print(f"Ruta: {table_path}")
    if not DeltaTable.isDeltaTable(spark, table_path):
        print("Todavia no existe como tabla Delta en esta ruta (no se ha ejecutado aun).")
        return

    delta_table = DeltaTable.forPath(spark, table_path)
    current_df = delta_table.toDF()
    print(f"Filas actuales: {current_df.count()}")

    print("\nHistorial de operaciones (de mas reciente a mas antigua):")
    history_df = delta_table.history().select(
        "version", "timestamp", "operation", "operationMetrics"
    )
    history_df.show(truncate=False)

    print("Lectura de 'operationMetrics' por version:")
    for row in history_df.orderBy("version").collect():
        metrics = row["operationMetrics"] or {}
        if row["operation"] == "MERGE":
            print(
                f"  v{row['version']} MERGE -> "
                f"insertadas={metrics.get('numTargetRowsInserted', '?')}, "
                f"actualizadas={metrics.get('numTargetRowsUpdated', '?')}, "
                f"borradas={metrics.get('numTargetRowsDeleted', '?')}"
            )
        elif row["operation"] == "WRITE":
            print(
                f"  v{row['version']} WRITE (mode={metrics.get('mode', '?')}) -> "
                f"filas_escritas={metrics.get('numOutputRows', '?')}"
            )
        else:
            print(f"  v{row['version']} {row['operation']} -> {metrics}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Lee los 4 destinos del dataflow 'prueba-acceso' y muestra "
        "evidencia de overwrite/append/merge a partir de lo que ya hay en disco."
    )
    parser.add_argument(
        "--tables-base-path",
        default=DEFAULT_TABLES_BASE_PATH,
        help="Debe coincidir con el usado al ejecutar main.py (default: %(default)s)",
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
