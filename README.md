# sdggroup_challenge

Motor genérico de pipelines de datos, orientado por configuración (`metadata.json`), sobre PySpark + Delta Lake. Prueba técnica para SDG Group.

## Estructura

```
sdggroup_challenge/
├── README.md
├── requirements.txt            # pyspark, delta-spark, pydantic, pytest
├── .gitignore
├── conftest.py                 # añade la raíz del proyecto a sys.path + fixture `spark` compartida por los tests
├── main.py                     # CLI de entrada: pipeline, --dry-run y subcomando `check`
├── metadata.json                # dataflow de ejemplo ("prueba-acceso") usado en todas las ejecuciones
├── assets/                     # datos de entrada crudos, fuente de verdad (data2024.csv, data2025.csv)
├── data/                        # (gitignored) copia de trabajo local: input renombrado + outputs generados
├── engine/                     # motor genérico, dirigido por metadata.json
│   ├── cli.py                   # punto 1: argumentos del comando (--metadata, --year, --tables-base-path, --dry-run)
│   ├── metadata_schema.py       # punto 2: modelos Pydantic + validación estructural de metadata.json
│   ├── graph.py                  # punto 2: construcción del DAG en memoria (aristas de datos y de espera)
│   ├── topology.py                # detección de ciclos + orden topológico sobre el grafo combinado
│   ├── templating.py              # resolución genérica de `{{ variable }}` en config (p.ej. {{ year }})
│   ├── paths.py                    # rebase de paths pseudo-absolutos de metadata.json bajo la raíz del proyecto
│   ├── spark_session.py            # SparkSession única del programa (with_delta=False para saltar Ivy/Delta cuando no hace falta)
│   ├── readers.py                   # dispatch de lectura por `type`/`format` (spark.read...)
│   ├── transformations.py           # dispatch filter / add_fields / group
│   ├── writers.py                    # dispatch file+save_mode / table+save_mode (incluye merge vs. bootstrap Delta)
│   ├── executor.py                    # orquestador: resuelve y ejecuta cada dataflow (memorización, `waits`, fan-out con cache)
│   └── describe.py                     # --dry-run: describe el grafo mapeado + la instrucción Spark de cada nodo, sin Spark
├── checks/                     # punto 5 del enunciado, independientes del motor
│   ├── diff_rows.py             # comprobación (a): diff de filas 2024 -> 2025 sobre los CSV crudos
│   ├── data_quality.py           # comprobación (b): 'Ambos sexos' == 'Hombres' + 'Mujeres' por municipio
│   └── output/                    # (gitignored) CSVs de detalle generados por ambas comprobaciones
├── scripts/
│   └── verify_outputs.py        # evidencia del comportamiento overwrite/append/merge (punto 4) vía Delta history()
├── tests/                       # 64 tests, pytest
│   ├── fixtures.py                # metadata.json real ("prueba-acceso") reutilizado por los tests del motor
│   ├── test_cli.py                 # engine.cli (incluye --dry-run)
│   ├── test_metadata_schema.py      # engine.metadata_schema
│   ├── test_graph.py                 # engine.graph
│   ├── test_topology.py               # engine.topology
│   ├── test_paths.py                   # engine.paths
│   ├── test_describe.py                 # engine.describe + --dry-run end-to-end (no toca ficheros ni crea SparkSession)
│   ├── test_main_check_dispatch.py       # dispatch de `main.py check <subcomando>`
│   ├── test_diff_rows.py                  # checks/diff_rows.py
│   └── test_data_quality.py                # checks/data_quality.py
└── docs/
    ├── evidence/                 # salida real de verify_outputs.py (after_2024.log, after_2025.log) -- evidencia, no se regenera en CI
    └── arquitectura_detallada.pdf  # punto 6: documento de arquitectura (acompaña a la presentación, deck separado)
```

## Estado actual

Todos los puntos del enunciado están implementados, verificados y documentados.

**Punto 1 -- CLI** (`engine/cli.py`, `main.py`): `argparse` con `--metadata` (ruta a `metadata.json`), `--year` (4 dígitos, resuelve `{{ year }}`), `--tables-base-path` (por defecto `/data/demo/output/tables`) y `--dry-run`. Cada fallo de validación propio (fichero inexistente/no legible, año inválido, base path en blanco) devuelve un `exit_code` distinto vía `CliValidationError`; los flags mal formados los gestiona `argparse` (exit code 2).

**Punto 2 -- metadata.json y DAG** (`engine/metadata_schema.py`, `engine/graph.py`, `engine/topology.py`): modelos Pydantic discriminados por `type` para `inputs` (file), `transformations` (filter / add_fields / group) y `outputs` (file / table); validación estructural completa (secciones no vacías, nombres únicos, integridad referencial de `input`/`waits`). `build_graph(s)` construye el DAG en memoria separando aristas de datos y de espera; `topological_sort` detecta ciclos sobre el grafo combinado y devuelve el orden global de ejecución.

**Punto 3 -- ejecución** (`engine/executor.py`, `engine/readers.py`, `engine/transformations.py`, `engine/writers.py`): una única `SparkSession` para todo el programa; cada dataflow se resuelve desde sus outputs "hoja" hacia atrás, memorizando el DataFrame de cada nodo intermedio (y cacheándolo si alimenta a más de un consumidor) y forzando (`waits`) la escritura real de un output antes de que el input que depende de él se lea. Soporta `type=file` (cualquier formato que entienda `DataFrameReader`/`DataFrameWriter`) y `type=table` (siempre Delta, por path, sin metastore): `save_mode=append` escribe directo; `save_mode=merge` usa `DeltaTable.merge()` por `primary_key`, salvo en la primera ejecución (tabla Delta inexistente en ese path), donde hace un `overwrite` de bootstrap.

**Punto 4 -- overwrite / append / merge demostrado con 2024 y 2025**: ejecutado con `data2024.csv` y, después, con `data2025.csv` contra el mismo `metadata.json`. `scripts/verify_outputs.py` captura evidencia objetiva vía `DeltaTable.history()` (operationMetrics: filas insertadas/actualizadas, numOutputRows) comparando ambas ejecuciones, en `docs/evidence/after_2024.log` y `after_2025.log`. Proyecto comprimido tras cada ejecución (`sdggroup_challenge_input_2024_carlos_blazquez`, `sdgroup_challenge_input_2025_carlos_blazquez`).

**Punto 5 -- comprobaciones** (`checks/diff_rows.py`, `checks/data_quality.py`), sobre los CSV crudos de `assets/` (no sobre los outputs del pipeline), reutilizando `engine.spark_session.create_spark_session(..., with_delta=False)` para no pagar la resolución de Ivy/Delta cuando no hace falta:
  - **(a) diff de filas**: clave `(provincia, municipio, sexo)`, `full_outer` join 2024 vs 2025, clasifica cada fila como `changed` / `added` / `removed` (descarta `unchanged`); el CSV de detalle incluye la clave, el tipo de cambio y el valor antiguo y nuevo de `total`.
  - **(b) calidad de dato**: para cada `(provincia, municipio)`, verifica que `Ambos sexos == Hombres + Mujeres`; reporta `sum_mismatch` / `missing_category` por fila, más un conteo total (exit code `20` si hay incumplimientos).
  - Ambas expuestas también vía CLI: `python main.py check diff-rows` / `python main.py check data-quality` (ver más abajo). El dispatch intercepta `check` *antes* de `parse_cli_args`, así que no afecta en nada a la validación ni a los tests de una ejecución normal del pipeline.

**`--dry-run`** (`engine/describe.py`): imprime, sin crear `SparkSession` ni ejecutar ninguna lectura/transformación/escritura, los nodos del grafo mapeado agrupados (inputs / transformaciones / outputs), con sus paths resueltos (templating incluido), y la instrucción Spark literal que cada nodo construiría y ejecutaría en un run real (`spark.read...`, `df.filter(...)`, `df.write...`, el `DeltaTable.merge()`/bootstrap de un output `table`), además del orden topológico de ejecución. `engine/describe.py` está deliberadamente libre de cualquier import de `pyspark` (ni siquiera transitivo) para no pagar el arranque de Spark solo por imprimir un plan.

**Tests** (`tests/`, 64 en total, `pytest`): cubren `engine.cli` (incluido `--dry-run`), `engine.metadata_schema`, `engine.graph`, `engine.topology`, `engine.paths`, `engine.describe` (más un end-to-end que verifica que `--dry-run` nunca crea una `SparkSession` ni toca `mtime`/tamaño de ningún fichero de salida existente), el dispatch de `main.py check <subcomando>`, y ambas comprobaciones del punto 5 (con fixtures que escriben un CSV real en `tmp_path` y lo leen vía la función de carga ya probada, en vez de `spark.createDataFrame()` -- necesario en Python 3.14, donde el `cloudpickle` que trae `pyspark==3.5.1` no sabe serializar el cierre que esa llamada necesita).

**Punto 6 -- documento de arquitectura**: dos entregables. Una presentación con los puntos más importantes (deck de diapositivas) y un documento detallado, `docs/arquitectura_detallada.pdf`, con la explicación de cada punto -- incluyendo el resultado real de los 3 tipos de comando `main.py` (ejecución estándar, `--dry-run`, `check`) -- y la arquitectura extendida sobre GCP (Dataproc Serverless, GCS, BigQuery) con Cloud Composer (Airflow) como orquestador y el pipeline de CI/CD correspondiente, sin IaC (fuera del alcance de este documento, según pide el propio enunciado).

## Uso

```bash
# 1. Crear entorno y dependencias
python -m venv env
env\Scripts\activate          # Windows; en Linux/macOS: source env/bin/activate
pip install -r requirements.txt

# 2. Ejecutar el pipeline completo
python main.py --metadata metadata.json --year 2024
python main.py --metadata metadata.json --year 2025

# 3. Ver el plan sin ejecutar nada (nodos, instrucción Spark de cada uno, orden topológico)
python main.py --metadata metadata.json --year 2025 --dry-run

# 4. Comprobaciones del punto 5, sobre los CSV crudos de assets/
python main.py check diff-rows [--input-2024 PATH] [--input-2025 PATH] [--output PATH]
python main.py check data-quality [--input PATH] [--output PATH]

# 5. Evidencia de overwrite/append/merge (después de haber ejecutado con 2024 y con 2025)
python scripts/verify_outputs.py

# 6. Tests
pytest -v
```
