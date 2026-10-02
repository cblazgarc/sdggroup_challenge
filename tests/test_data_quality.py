"""Tests for checks.data_quality: 'Ambos sexos' == 'Hombres' + 'Mujeres' validation."""
from checks.data_quality import compute_quality_issues, load_population_df, summarize


def _rows(spark, tmp_path, name, data):
    """
    Build a tiny DataFrame by writing a real CSV to `tmp_path` and reading
    it through `load_population_df` -- the same path `main.py` uses in
    production (pure Spark/JVM CSV reading).

    Deliberately NOT `spark.createDataFrame(data, schema=[...])`: on
    Python 3.14, pyspark 3.5.1's bundled `cloudpickle` cannot serialize the
    Python-side closure that path needs (even for local execution), which
    surfaces as a runaway recursion instead of a clean error.
    `load_population_df` never ships a Python closure to Spark, so it's
    unaffected regardless of the interpreter version.
    """
    path = tmp_path / name
    lines = ["provincia;municipio;sexo;total"]
    lines.extend(f"{provincia};{municipio};{sexo};{total}" for provincia, municipio, sexo, total in data)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return load_population_df(spark, str(path))


def test_compute_quality_issues_flags_sum_mismatch(spark, tmp_path):
    df = _rows(
        spark,
        tmp_path,
        "rows.csv",
        [
            ("P1", "M1", "Hombres", 10),
            ("P1", "M1", "Mujeres", 5),
            ("P1", "M1", "Ambos sexos", 16),  # should be 15
        ],
    )

    issues = compute_quality_issues(df).collect()

    assert len(issues) == 1
    assert issues[0]["issue"] == "sum_mismatch"
    assert issues[0]["hombres_mas_mujeres"] == 15
    assert issues[0]["ambos_sexos"] == 16
    assert issues[0]["diff"] == 1


def test_compute_quality_issues_flags_missing_category(spark, tmp_path):
    df = _rows(
        spark,
        tmp_path,
        "rows.csv",
        [
            ("P1", "M1", "Hombres", 10),
            ("P1", "M1", "Ambos sexos", 10),
            # "Mujeres" missing entirely for M1
        ],
    )

    issues = compute_quality_issues(df).collect()

    assert len(issues) == 1
    assert issues[0]["issue"] == "missing_category"
    assert issues[0]["mujeres"] is None


def test_compute_quality_issues_passes_consistent_data(spark, tmp_path):
    df = _rows(
        spark,
        tmp_path,
        "rows.csv",
        [
            ("P1", "M1", "Hombres", 10),
            ("P1", "M1", "Mujeres", 5),
            ("P1", "M1", "Ambos sexos", 15),
        ],
    )

    issues = compute_quality_issues(df).collect()

    assert issues == []


def test_summarize_counts_issue_types(spark, tmp_path):
    df = _rows(
        spark,
        tmp_path,
        "rows.csv",
        [
            ("P1", "M1", "Hombres", 10),
            ("P1", "M1", "Mujeres", 5),
            ("P1", "M1", "Ambos sexos", 16),  # sum_mismatch
            ("P2", "M2", "Hombres", 3),
            ("P2", "M2", "Ambos sexos", 3),  # missing_category (Mujeres)
        ],
    )
    issues_df = compute_quality_issues(df)

    summary = summarize(2, issues_df)

    assert "checked (provincia+municipio): 2" in summary
    assert "'Ambos sexos' != 'Hombres' + 'Mujeres': 1" in summary
    assert "missing for some municipio: 1" in summary
    assert "Total issues detected: 2" in summary
