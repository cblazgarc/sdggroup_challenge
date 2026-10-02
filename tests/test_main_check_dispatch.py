"""
Tests for the `check` subcommand dispatch in main.py.

Only covers the dispatch boundary itself (missing/unknown subcommand) --
the actual logic of each check is already covered by
tests/test_diff_rows.py and tests/test_data_quality.py, and re-running it
through a real Spark session here would just duplicate that coverage
slower. No mocks: these two paths return before ever touching Spark or
checks/, so they exercise main.py's real code as-is.
"""
import main


def test_check_without_subcommand_reports_usage_error():
    exit_code = main.main(["check"])

    assert exit_code == main.EXIT_CHECK_USAGE


def test_check_with_unknown_subcommand_reports_usage_error():
    exit_code = main.main(["check", "not-a-real-subcommand"])

    assert exit_code == main.EXIT_CHECK_USAGE
