"""
CLI argument processing for the launch command.

Responsibility: define and parse the command-line arguments the engine
needs to run (path to ``metadata.json``, the ``year`` used to resolve the
``{{ year }}`` templating in input paths, and the base path under which
``type=table`` outputs are resolved), and validate them with clear error
messages and distinct, non-zero exit codes on failure.

Nothing here touches the metadata schema, the graph, or Spark.
"""
from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path

DEFAULT_TABLES_BASE_PATH = "/data/demo/output/tables"

# Exit codes. 0 is success; argparse itself exits with 2 for malformed/missing
# flags (e.g. a required flag not supplied). The codes below are for
# arguments that parse syntactically but fail a domain validation, so a
# caller (main.py) can distinguish each failure mode.
EXIT_OK = 0
EXIT_METADATA_NOT_FOUND = 10
EXIT_METADATA_NOT_READABLE = 11
EXIT_INVALID_YEAR = 12
EXIT_INVALID_TABLES_BASE_PATH = 13

_YEAR_PATTERN = re.compile(r"^\d{4}$")


class CliValidationError(Exception):
    """
    Raised when a CLI argument parses syntactically but fails validation.

    Carries `exit_code`, a non-zero code identifying which validation
    failed, so the caller can `sys.exit` with something more specific than
    argparse's blanket 2.
    """

    def __init__(self, message: str, exit_code: int):
        super().__init__(message)
        self.exit_code = exit_code


@dataclass(frozen=True)
class ParsedArgs:
    """Validated launch arguments."""

    metadata_path: Path
    year: int
    tables_base_path: str
    dry_run: bool = False


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sdggroup_challenge",
        description="Generic PySpark/Delta Lake pipeline engine driven by metadata.json.",
    )
    parser.add_argument(
        "--metadata",
        required=True,
        metavar="PATH",
        help="Path to the metadata.json file describing the pipeline's dataflows.",
    )
    parser.add_argument(
        "--year",
        required=True,
        metavar="YYYY",
        help="4-digit year used to resolve the '{{ year }}' templating in input paths.",
    )
    parser.add_argument(
        "--tables-base-path",
        default=DEFAULT_TABLES_BASE_PATH,
        metavar="PATH",
        help=f"Base path under which type=table outputs are resolved (default: {DEFAULT_TABLES_BASE_PATH}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Parse and map the dataflow graph(s) (nodes, transformations, "
            "final actions, execution order) and print the result, but "
            "never create a SparkSession or execute any read/transform/write "
            "-- no existing output is touched."
        ),
    )
    return parser


def _validate_metadata_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if not path.exists():
        raise CliValidationError(f"metadata file not found: '{path}'", EXIT_METADATA_NOT_FOUND)
    if not path.is_file():
        raise CliValidationError(f"metadata path is not a file: '{path}'", EXIT_METADATA_NOT_FOUND)
    if not os.access(path, os.R_OK):
        raise CliValidationError(f"metadata file is not readable: '{path}'", EXIT_METADATA_NOT_READABLE)
    return path


def _validate_year(raw_year: str) -> int:
    candidate = raw_year.strip()
    if not _YEAR_PATTERN.match(candidate):
        raise CliValidationError(
            f"invalid --year value '{raw_year}': expected a 4-digit year, e.g. 2024",
            EXIT_INVALID_YEAR,
        )
    return int(candidate)


def _validate_tables_base_path(raw_path: str) -> str:
    if not raw_path.strip():
        raise CliValidationError("--tables-base-path must not be blank", EXIT_INVALID_TABLES_BASE_PATH)
    return raw_path


def parse_cli_args(argv: list[str] | None = None) -> ParsedArgs:
    """
    Parse and validate the launch command's arguments.

    Parameters
    ----------
    argv:
        Argument list to parse (excluding the program name). Defaults to
        `sys.argv[1:]` via argparse when None.

    Returns
    -------
    ParsedArgs
        The validated `metadata_path` (existing, readable file), `year`
        (int) and `tables_base_path` (str).

    Raises
    ------
    CliValidationError
        If an argument fails domain validation. Carries a distinct,
        non-zero `exit_code`.
    SystemExit
        Raised by argparse itself (exit code 2) for missing/malformed flags.
    """
    parser = build_arg_parser()
    namespace = parser.parse_args(argv)

    metadata_path = _validate_metadata_path(namespace.metadata)
    year = _validate_year(namespace.year)
    tables_base_path = _validate_tables_base_path(namespace.tables_base_path)

    return ParsedArgs(
        metadata_path=metadata_path,
        year=year,
        tables_base_path=tables_base_path,
        dry_run=namespace.dry_run,
    )
