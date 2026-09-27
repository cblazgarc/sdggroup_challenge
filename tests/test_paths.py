"""Tests for engine.paths: rebasing metadata.json's pseudo-absolute paths under the project root."""
from engine.paths import PROJECT_ROOT, resolve_project_path


def test_leading_slash_path_is_rebased_under_project_root():
    resolved = resolve_project_path("/data/demo/input/poblacion2024.csv")

    assert resolved == str(PROJECT_ROOT / "data" / "demo" / "input" / "poblacion2024.csv")


def test_leading_backslash_path_is_rebased_under_project_root():
    resolved = resolve_project_path("\\data\\demo\\output\\last")

    assert resolved == str(PROJECT_ROOT / "data" / "demo" / "output" / "last")


def test_windows_drive_letter_path_is_left_untouched():
    resolved = resolve_project_path(r"C:\custom\tables")

    assert resolved == r"C:\custom\tables"


def test_already_relative_path_is_left_untouched():
    resolved = resolve_project_path("relative/data/path.csv")

    assert resolved == "relative/data/path.csv"


def test_same_raw_path_resolves_identically_every_time():
    # This is the property that matters for `parquet_data` (an input,
    # waits=[write_last_file]) and `write_last_file` (an output): both
    # declare config.path="/data/demo/output/last" and MUST resolve to the
    # exact same filesystem location.
    input_side = resolve_project_path("/data/demo/output/last")
    output_side = resolve_project_path("/data/demo/output/last")

    assert input_side == output_side
