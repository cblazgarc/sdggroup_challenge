"""Tests for engine.cli: argument parsing and validation."""
import pytest

from engine.cli import (
    DEFAULT_TABLES_BASE_PATH,
    EXIT_INVALID_TABLES_BASE_PATH,
    EXIT_INVALID_YEAR,
    EXIT_METADATA_NOT_FOUND,
    EXIT_METADATA_NOT_READABLE,
    CliValidationError,
    parse_cli_args,
)


def _write_metadata(tmp_path):
    metadata_file = tmp_path / "metadata.json"
    metadata_file.write_text("{}")
    return metadata_file


def test_parse_valid_args_uses_default_tables_base_path(tmp_path):
    metadata_file = _write_metadata(tmp_path)

    args = parse_cli_args(["--metadata", str(metadata_file), "--year", "2024"])

    assert args.metadata_path == metadata_file
    assert args.year == 2024
    assert args.tables_base_path == DEFAULT_TABLES_BASE_PATH


def test_parse_valid_args_with_custom_tables_base_path(tmp_path):
    metadata_file = _write_metadata(tmp_path)

    args = parse_cli_args(
        [
            "--metadata", str(metadata_file),
            "--year", "2025",
            "--tables-base-path", "/custom/tables",
        ]
    )

    assert args.year == 2025
    assert args.tables_base_path == "/custom/tables"


def test_missing_metadata_file_fails_with_distinct_exit_code(tmp_path):
    missing_path = tmp_path / "does_not_exist.json"

    with pytest.raises(CliValidationError) as exc_info:
        parse_cli_args(["--metadata", str(missing_path), "--year", "2024"])

    assert exc_info.value.exit_code == EXIT_METADATA_NOT_FOUND


def test_metadata_path_pointing_to_a_directory_fails(tmp_path):
    with pytest.raises(CliValidationError) as exc_info:
        parse_cli_args(["--metadata", str(tmp_path), "--year", "2024"])

    assert exc_info.value.exit_code == EXIT_METADATA_NOT_FOUND


@pytest.mark.parametrize("bad_year", ["24", "20245", "abcd", "", "202a"])
def test_invalid_year_fails_with_distinct_exit_code(tmp_path, bad_year):
    metadata_file = _write_metadata(tmp_path)

    with pytest.raises(CliValidationError) as exc_info:
        parse_cli_args(["--metadata", str(metadata_file), "--year", bad_year])

    assert exc_info.value.exit_code == EXIT_INVALID_YEAR


def test_blank_tables_base_path_fails(tmp_path):
    metadata_file = _write_metadata(tmp_path)

    with pytest.raises(CliValidationError) as exc_info:
        parse_cli_args(
            [
                "--metadata", str(metadata_file),
                "--year", "2024",
                "--tables-base-path", "   ",
            ]
        )

    assert exc_info.value.exit_code == EXIT_INVALID_TABLES_BASE_PATH


def test_missing_required_flags_raises_systemexit_via_argparse():
    with pytest.raises(SystemExit):
        parse_cli_args([])
