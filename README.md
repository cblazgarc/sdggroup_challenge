# sdggroup_challenge

Motor genérico de pipelines de datos, orientado por configuración (`metadata.json`), sobre PySpark + Delta Lake.

## Estructura

```
sdggroup_challenge/
├── README.md                 # este fichero
├── requirements.txt          # pyspark, delta-spark, pydantic, pytest
├── .gitignore
├── conftest.py                # añade la raíz del proyecto a sys.path para los tests
├── main.py                    # CLI de entrada
├── assets/                    # datos de entrada (data2024.csv, data2025.csv)
├── engine/                    # motor genérico
│   ├── cli.py                  # punto 1: procesamiento y validación de argumentos del comando de lanzamiento
│   ├── metadata_schema.py      # punto 2: modelos Pydantic + validación estructural de metadata.json
│   ├── graph.py                 # punto 2: construcción de la estructura DAG en memoria a partir del metadata validado
│   ├── registry.py              # (pendiente) indexa nodos por name
│   ├── readers.py               # (pendiente) dispatch de lectura por format
│   ├── transformations.py       # (pendiente) dispatch filter/add_fields/group
│   ├── writers.py                # (pendiente) dispatch file+save_mode / table+save_mode
│   └── engine.py                  # (pendiente) orquestador: topological sort, SparkSession, ejecución
├── checks/
│   ├── diff_rows.py             # (pendiente) comprobación (a)
│   └── data_quality.py           # (pendiente) comprobación (b)
├── tests/
│   ├── fixtures.py               # metadata.json real ("prueba-acceso") reutilizado por los tests
│   ├── test_cli.py                # tests de engine.cli
│   ├── test_metadata_schema.py    # tests de engine.metadata_schema
│   └── test_graph.py               # tests de engine.graph
└── docs/                      # aquí irá el .pptx de arquitectura
```

## Estado actual (puntos 1 y 2)

Implementado:

1. **Procesamiento de argumentos del comando de lanzamiento** (`engine/cli.py`): CLI con `argparse` que recibe `--metadata` (ruta al `metadata.json`), `--year` (4 dígitos, resuelve el templating `{{ year }}` de los paths) y `--tables-base-path` (por defecto `/data/demo/output/tables`). Cada fallo de validación (fichero inexistente/no legible, año inválido, base path en blanco) lanza `CliValidationError` con un `exit_code` propio y distinto de 0; los flags mal formados o ausentes los gestiona `argparse` (exit code 2).

2. **Parseo de `metadata.json`** (`engine/metadata_schema.py` + `engine/graph.py`):
   - Modelos Pydantic discriminados por `type` para `inputs` (file), `transformations` (filter / add_fields / group) y `outputs` (file / table), cada uno con el shape de `config` que le corresponde (incluida la regla `primary_key` obligatorio solo si `save_mode=merge` en outputs de tipo `table`).
   - Validación estructural sobre el fichero completo: `inputs`/`outputs` no vacíos, `transformations` opcional, unicidad global de `name` entre las tres secciones, e integridad referencial de todo `input` y `waits` contra nombres existentes — con mensajes de error explícitos nombrando el nodo y la referencia rota.
   - `build_graph`/`build_graphs` construyen la estructura DAG en memoria: aristas de datos (`producer -> consumer`, una por cada `input`) y aristas de espera (`waited_node -> waiting_node`, una por cada entrada en `waits`) mantenidas en listas separadas, para que el futuro resolver de ejecución pueda distinguirlas sin ambigüedad.

Pendiente (pasos siguientes): resolución del templating genérico de `config` contra los argumentos (`{{ year }}` y cualquier otro), creación de la SparkSession, detección de ciclos y orden topológico combinando aristas de datos y de espera, y la ejecución real (readers/transformations/writers).

## Uso previsto

```bash
# 1. Crear entorno y dependencias
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# 2. Parsear y validar metadata.json (no ejecuta el pipeline todavía)
python main.py --metadata metadata.json --year 2024
python main.py --metadata metadata.json --year 2025 --tables-base-path /data/demo/output/tables

# 3. Tests
pytest tests/ -v
```
