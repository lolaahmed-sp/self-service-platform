from __future__ import annotations

import argparse
from pathlib import Path

from pipeline_platform.config_parser import load_pipeline_config
from pipeline_platform.pipeline_generator import PipelineExecutor, generate_dag_file
from pipeline_platform.warehouse.duckdb_client import DuckDBWarehouse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Self-Service Data Pipeline Platform")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a single pipeline config")
    run_parser.add_argument("--config", required=True, help="Path to YAML config")

    run_all_parser = subparsers.add_parser("run-all", help="Run all pipeline configs in a directory")
    run_all_parser.add_argument("--config-dir", required=True, help="Directory containing YAML configs")

    dag_parser = subparsers.add_parser("generate-dag", help="Generate DAG stub from config")
    dag_parser.add_argument("--config", required=True, help="Path to YAML config")

    show_parser = subparsers.add_parser("show-table", help="Show table contents from DuckDB")
    show_parser.add_argument("--table", required=True, help="Table name to inspect")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    warehouse = DuckDBWarehouse(db_path="warehouse.duckdb")

    if args.command == "run":
        config = load_pipeline_config(args.config)
        executor = PipelineExecutor(warehouse=warehouse)
        executor.execute(config)
        print(f"Pipeline '{config.pipeline_name}' executed successfully.")

    elif args.command == "run-all":
        executor = PipelineExecutor(warehouse=warehouse)
        for config_path in sorted(Path(args.config_dir).glob("*.yaml")):
            config = load_pipeline_config(str(config_path))
            executor.execute(config)
            print(f"Executed: {config.pipeline_name}")

    elif args.command == "generate-dag":
        config = load_pipeline_config(args.config)
        output_path = generate_dag_file(config)
        print(f"Generated DAG file: {output_path}")

    elif args.command == "show-table":
        print(warehouse.fetch_table(args.table).to_string(index=False))


if __name__ == "__main__":
    main()
