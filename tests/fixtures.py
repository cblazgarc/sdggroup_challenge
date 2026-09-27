"""Shared fixture data for the metadata_schema and graph tests: the real 'prueba-acceso' dataflow."""
import copy

VALID_METADATA: dict = {
    "dataflows": [
        {
            "name": "prueba-acceso",
            "inputs": [
                {
                    "name": "demo_data",
                    "type": "file",
                    "config": {
                        "path": "/data/demo/input/poblacion{{ year }}.csv",
                        "format": "csv",
                    },
                    "options": {
                        "header": "true",
                        "delimiter": ";",
                    },
                },
                {
                    "name": "parquet_data",
                    "type": "file",
                    "config": {
                        "path": "/data/demo/output/last",
                        "format": "parquet",
                    },
                    "waits": ["write_last_file"],
                },
            ],
            "transformations": [
                {
                    "name": "filter_rows",
                    "type": "filter",
                    "input": "new_fields",
                    "config": {
                        "filter": "sexo != 'Ambos sexos' and municipio != 'N/A'",
                    },
                },
                {
                    "name": "new_fields",
                    "type": "add_fields",
                    "input": "demo_data",
                    "config": {
                        "fields": [
                            {"name": "domain", "expression": "'demography'"},
                            {"name": "load_date", "expression": "current_date()"},
                        ],
                    },
                },
                {
                    "name": "group_by_fields",
                    "type": "group",
                    "input": "parquet_data",
                    "config": {
                        "group_fields": ["provincia", "municipio", "sexo"],
                        "aggregations": ["sum(total) as total_ambos_sexos"],
                    },
                },
            ],
            "outputs": [
                {
                    "name": "write_last_file",
                    "type": "file",
                    "input": "filter_rows",
                    "config": {
                        "path": "/data/demo/output/last",
                        "format": "parquet",
                        "save_mode": "overwrite",
                    },
                },
                {
                    "name": "write_historic_file",
                    "type": "file",
                    "input": "filter_rows",
                    "config": {
                        "path": "/data/demo/output/historic",
                        "format": "parquet",
                        "save_mode": "append",
                        "partition": "load_date",
                    },
                },
                {
                    "name": "write_delta_merge",
                    "type": "table",
                    "input": "group_by_fields",
                    "config": {
                        "table": "demo",
                        "save_mode": "merge",
                        "primary_key": ["provincia", "municipio", "sexo"],
                    },
                },
                {
                    "name": "write_delta_raw",
                    "type": "table",
                    "input": "group_by_fields",
                    "config": {
                        "table": "raw_demo",
                        "save_mode": "append",
                    },
                },
            ],
        }
    ]
}


def clone_valid_metadata() -> dict:
    """Deep copy of VALID_METADATA, safe for tests to mutate."""
    return copy.deepcopy(VALID_METADATA)
