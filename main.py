#!/usr/bin/env python
"""
CLI entry point for sdggroup_challenge.

Scope covered so far:
  1. Parse and validate the launch command's arguments (see engine.cli).
  2. Parse metadata.json into validated Pydantic models and build the
     in-memory DAG structure for each dataflow (see engine.metadata_schema
     and engine.graph).
  3. Create the single SparkSession for the whole program, configured for
     path-based Delta Lake (see engine.spark_session).
  4. For each dataflow: validate it (cycle detection + global topological
     sort over the combined data+wait graph) and execute it — reader ->
     transformations -> writer per node, with memoization across branches
     and `waits` forcing upstream writes (see engine.topology and
     engine.executor).
"""
from __future__ import annotations

import sys

from engine.cli import CliValidationError, parse_cli_args
from engine.executor import RunContext, resolve_all
from engine.graph import build_graphs
from engine.metadata_schema import MetadataError, load_metadata
from engine.spark_session import create_spark_session
from engine.topology import GraphCycleError

EXIT_METADATA_SCHEMA_INVALID = 14
EXIT_GRAPH_CYCLE = 15


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

    # A single SparkSession for the whole program, reused across every dataflow.
    spark = create_spark_session()
    try:
        run_context = RunContext(spark=spark, year=args.year, tables_base_path=args.tables_base_path)

        try:
            topo_orders = resolve_all(graphs, run_context)
        except GraphCycleError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return EXIT_GRAPH_CYCLE

        for dataflow_name, topo_order in topo_orders.items():
            print(f"Dataflow '{dataflow_name}' executed. Topological order: {' -> '.join(topo_order)}")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
