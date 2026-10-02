"""Tests for checks.diff_rows: change/add/remove detection and summary reporting."""
from checks.diff_rows import compute_diff, load_population_df, summarize


def _rows(spark, tmp_path, name, data):
    """
    Build a tiny DataFrame by writing a real CSV to `tmp_path` and reading
    it through `load_population_df` -- the same path `main.py` uses in
    production (pure Spark/JVM CSV reading).

    Deliberately NOT `spark.createDataFrame(data, schema=[...])`: on
    Python 3.14, pyspark 3.5.1's bundled `cloudpickle` cannot serialize the
    Python-side closure that path needs (even for local execution), which
    surfaces as a runaway recursion ("when serializing function object" /
    "when serializing function reconstructor" repeated thousands of
    times) instead of a clean error. `load_population_df` never ships a
    Python closure to Spark, so it's unaffected regardless of the
    interpreter version.
    """
    path = tmp_path / name
    lines = ["provincia;municipio;sexo;total"]
    lines.extend(f"{provincia};{municipio};{sexo};{total}" for provincia, municipio, sexo, total in data)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return load_population_df(spark, str(path))


def test_compute_diff_detects_changed_added_and_removed_rows(spark, tmp_path):
    old_df = _rows(
        spark,
        tmp_path,
        "old.csv",
        [
            ("P1", "M1", "Hombres", 10),
            ("P1", "M2", "Hombres", 5),
        ],
    )
    new_df = _rows(
        spark,
        tmp_path,
        "new.csv",
        [
            ("P1", "M1", "Hombres", 12),  # changed: 10 -> 12
            ("P1", "M3", "Hombres", 7),  # added: new key in 2025
            # ("P1", "M2", ...) removed: present only in 2024
        ],
    )

    diffs = compute_diff(old_df, new_df).collect()
    by_key = {(r["provincia"], r["municipio"], r["sexo"]): r for r in diffs}

    assert len(diffs) == 3
    assert by_key[("P1", "M1", "Hombres")]["status"] == "changed"
    assert by_key[("P1", "M1", "Hombres")]["delta"] == 2
    assert by_key[("P1", "M3", "Hombres")]["status"] == "added"
    assert by_key[("P1", "M3", "Hombres")]["total_2024"] is None
    assert by_key[("P1", "M2", "Hombres")]["status"] == "removed"
    assert by_key[("P1", "M2", "Hombres")]["total_2025"] is None


def test_compute_diff_excludes_unchanged_rows(spark, tmp_path):
    old_df = _rows(spark, tmp_path, "old.csv", [("P1", "M1", "Hombres", 10), ("P1", "M2", "Mujeres", 3)])
    new_df = _rows(spark, tmp_path, "new.csv", [("P1", "M1", "Hombres", 10), ("P1", "M2", "Mujeres", 3)])

    diffs = compute_diff(old_df, new_df).collect()

    assert diffs == []


def test_summarize_reports_counts_and_population_totals(spark, tmp_path):
    old_df = _rows(spark, tmp_path, "old.csv", [("P1", "M1", "Hombres", 10), ("P1", "M2", "Hombres", 5)])
    new_df = _rows(spark, tmp_path, "new.csv", [("P1", "M1", "Hombres", 12), ("P1", "M3", "Hombres", 7)])
    diffs_df = compute_diff(old_df, new_df)

    summary = summarize(old_df, new_df, diffs_df)

    assert "Filas en 2024: 2" in summary
    assert "Filas en 2025: 2" in summary
    assert "Filas cambiadas (mismo municipio+sexo, total distinto): 1" in summary
    assert "Filas nuevas en 2025: 1" in summary
    assert "Filas eliminadas respecto a 2024: 1" in summary
    assert "Poblacion total 2024: 15" in summary
    assert "Poblacion total 2025: 19" in summary
    assert "Variacion neta: +4" in summary


def test_load_population_df_parses_thousands_separator_and_bom(spark, tmp_path):
    """
    Reproduces the two real quirks of the challenge's CSVs: a leading UTF-8
    BOM and '.' as a thousands separator (not a decimal point) in `total`.
    """
    csv_path = tmp_path / "poblacion_test.csv"
    csv_path.write_bytes(
        (
            "﻿provincia;municipio;sexo;total\n"
            "02 Albacete;02001 Abengibre;Hombres;1.932\n"
        ).encode("utf-8")
    )

    df = load_population_df(spark, str(csv_path))
    row = df.collect()[0]

    assert row["provincia"] == "02 Albacete"
    assert row["municipio"] == "02001 Abengibre"
    assert row["total"] == 1932
