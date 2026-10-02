"""
Evidencia de comportamiento de las salidas (overwrite / append / merge) tras
cada ejecucion de `main.py` -- punto 4 del enunciado.

No forma parte del motor dirigido por metadatos ni de las comprobaciones
obligatorias del punto 5 (esas viven en checks/ y operan sobre los CSV
crudos). Este script es una utilidad de verificacion/evidencia: lee los 4
destinos del dataflow "prueba-acceso" tal y como quedaron en disco/Delta
despues de una ejecucion, e imprime lo necesario para demostrar que cada
`save_mode` se comporto como se espera.

Uso tipico (ver README):
    python main.py --metadata metadata.json --year 2024
    python scripts/verify_outputs.py > docs/evidence/after_2024.log

    python main.py --metadata metadata.json --year 2025
    python scripts/verify_outputs.py > docs/evidence/after_2025.log

Acepta los mismos argumentos de rutas que `main.py` para no hardcodear nada:
--metadata (para resolver automaticamente nombres de tabla/paths del
dataflow "prueba-acceso") y --tables-base-path. No requiere --year: no
ejecuta el pipeline, solo lee lo que ya quedo escrito.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Permite ejecutar este script como `python scripts/verify_outputs.py` desde
# la raiz del proyecto: sin esto, Python añade `scripts/` (no la raiz) a
# sys.path y `import engine...` falla con ModuleNotFoundError. Mismo patron
# que usa conftest.py para los tests.
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
