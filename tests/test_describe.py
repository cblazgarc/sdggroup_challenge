"""
Tests for engine.describe (`--dry-run`'s rendering) and for the `--dry-run`
CLI path end to end.

engine.describe is pure Python (no Spark import at all -- see its module
docstring), so the unit tests below build the real 'prueba-acceso' graph
directly from tests.fixtures, exactly like test_graph.py/test_topology.py
do, and never need a `spark` fixture.

The CLI-level test does go through main.main([...]) with the real
metadata.json/assets on disk, to prove the actual guarantee Carlos asked
for: `--dry-run` must never create a SparkSession or touch an existing
output file.
"""
from pathlib import Path

import pytest

from engine.describe import describe_dataflow
from engine.graph import build_graph
from engine.metadata_schema import MetadataFile
from engine.templating import build_execution_context
from engine.topology import topological_sort
from tests.fixtures import clone_valid_metadata

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _build_prueba_acceso_graph():
    metadata = MetadataFile.model_validate(clone_valid_metadata())
    return build_graph(metadata.dataflows[0])


def _describe_prueba_acceso(year: int = 2025, tables_base_path: str = "/data/demo/output/tables") -> str:
    graph = _build_prueba_acceso_graph()
    topo_order = topological_sort(graph)
    template_context = build_execution_context(year)
    return describe_dataflow(graph, topo_order, template_context, tables_base_path)


def test_describe_dataflow_lists_every_node_once():
    graph = _build_prueba_acceso_graph()
    report = _describe_prueba_acceso()

    for node_name in graph.nodes:
        assert node_name in report


def test_describe_dataflow_resolves_the_year_template_in_input_paths():
    report = _describe_prueba_acceso(year=2025)

    assert "path (resolved) = " in report and "poblacion2025.csv" in report
    # the un-templated original is still shown for context, labeled separately
    assert "path (template) = /data/demo/input/poblacion{{ year }}.csv" in report


def test_describe_dataflow_resolves_table_output_against_tables_base_path():
    report = _describe_prueba_acceso(tables_base_path="/custom/tables")

    # write_delta_raw/write_delta_merge's `config.table` carries no path of
    # its own -- it must be resolved against tables_base_path, same as a
    # real run would via engine.writers.resolve_table_path.
    assert "/custom/tables/raw_demo" in report
    assert "/custom/tables/demo" in report


def test_describe_dataflow_marks_outputs_as_final_actions_and_shows_execution_order():
    report = _describe_prueba_acceso()

    assert report.count("<-- final action") == 4  # write_last_file, write_historic_file, write_delta_raw, write_delta_merge
    assert "Execution order (topological):" in report


def test_describe_dataflow_shows_the_wait_dependency():
    report = _describe_prueba_acceso()

    assert "waits for       = write_last_file" in report


def test_describe_dataflow_renders_the_spark_instruction_per_node():
    """
    Carlos asked specifically to see the final Spark instruction each node
    would build and execute (not just its config) -- one rendered call per
    node, mirroring engine.readers/engine.transformations/engine.writers'
    exact call shape (never by actually calling them: engine.describe
    stays Spark-free, see its module docstring).
    """
    report = _describe_prueba_acceso(tables_base_path="/data/demo/output/tables")

    # inputs: spark.read...
    assert 'spark.read.format("csv").options(header="true", delimiter=";").load(' in report
    assert 'spark.read.format("parquet").load(' in report

    # transformations: chained on the producer node's name
    assert 'demo_data.withColumn("domain", F.expr("\'demography\'"))' in report
    assert 'new_fields.filter("sexo != \'Ambos sexos\' and municipio != \'N/A\'")' in report
    assert 'parquet_data.groupBy("provincia", "municipio", "sexo").agg(F.expr("sum(total) as total_ambos_sexos"))' in report

    # file outputs: df.write...
    assert 'filter_rows.write.format("parquet").mode("append").partitionBy("load_date").save(' in report
    assert 'filter_rows.write.format("parquet").mode("overwrite").save(' in report

    # table output, append: plain delta write
    assert 'group_by_fields.write.format("delta").mode("append").save(' in report

    # table output, merge: both real-run branches shown (bootstrap vs. merge),
    # since deciding between them needs a live Delta check --dry-run can't do
    assert "if DeltaTable.isDeltaTable(spark," in report
    assert '.merge(group_by_fields.alias("source"), "target.`provincia` = source.`provincia` AND target.`municipio` = source.`municipio` AND target.`sexo` = source.`sexo`")' in report
    assert ".whenMatchedUpdateAll()" in report
    assert ".whenNotMatchedInsertAll()" in report
    assert 'group_by_fields.write.format("delta").mode("overwrite").save(' in report


@pytest.mark.parametrize("year", [2024, 2025])
def test_dry_run_never_touches_existing_output_files_or_creates_a_spark_session(year, monkeypatch):
    """
    End-to-end guarantee Carlos asked for: `python main.py --metadata
    metadata.json --year <YYYY> --dry-run` must only print the mapped graph
    and never execute a read/transform/write. We assert this two ways:
    1. `create_spark_session` is monkeypatched to raise if ever called --
       --dry-run must return before reaching it.
    2. Every file already under data/demo/output/ keeps its exact mtime
       and size after the dry run.
    """
    import main

    output_root = PROJECT_ROOT / "data" / "demo" / "output"
    before = {
        path: (path.stat().st_mtime_ns, path.stat().st_size)
        for path in output_root.rglob("*")
        if path.is_file()
    }

    def _fail_if_called(*args, **kwargs):
        raise AssertionError("--dry-run must never create a SparkSession")

    monkeypatch.setattr(main, "create_spark_session", _fail_if_called)

    exit_code = main.main(
        [
            "--metadata", str(PROJECT_ROOT / "metadata.json"),
            "--year", str(year),
            "--dry-run",
        ]
    )

    assert exit_code == 0

    after = {
        path: (path.stat().st_mtime_ns, path.stat().st_size)
        for path in output_root.rglob("*")
        if path.is_file()
    }
    assert before == after
