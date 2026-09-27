#!/usr/bin/env python
"""
CLI entry point for sdggroup_challenge.

Current scope (steps 1 and 2 of the engine build-out):
  1. Parse and validate the launch command's arguments (see engine.cli).
  2. Parse metadata.json into validated Pydantic models and build the
     in-memory DAG structure for each dataflow (see engine.metadata_schema
     and engine.graph).

Not implemented yet: cycle detection, topological sort, the SparkSession,
and actually reading/transforming/writing data. Those are later steps.
"""
from __future__ import annotations

import sys

from engine.cli import CliValidationError, parse_cli_args
from engine.graph import build_graphs
from engine.metadata_schema import MetadataError, load_metadata

EXIT_METADATA_SCHEMA_INVALID = 14


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_cli_args(argv)
    except CliValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return exc.exit_code

    try:
        metadata = load_metadata(args.metadata_path)
    except MetadataError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_METADATA_SCHEMA_INVALID

    graphs = build_graphs(metadata)

    print(f"Parsed metadata file: {args.metadata_path}")
    print(f"year={args.year} tables_base_path={args.tables_base_path}")
    for dataflow_name, graph in graphs.items():
        print(
            f"Dataflow '{dataflow_name}': {len(graph.nodes)} nodes "
            f"({len(graph.data_edges)} data edges, {len(graph.wait_edges)} wait edges)"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
