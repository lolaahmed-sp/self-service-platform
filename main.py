from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipeline_platform.config_parser import load_pipeline_config
from pipeline_platform.pipeline_generator import PipelineExecutor, generate_dag_file
from pipeline_platform.warehouse.duckdb_client import DuckDBWarehouse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Self-Service Data Pipeline Platform")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a single pipeline config")
    run_parser.add_argument("--config", required=True, help="Path to YAML config")
    run_parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Skip schema validation checks",
    )

    run_all_parser = subparsers.add_parser(
        "run-all", help="Run all pipeline configs in a directory"
    )
    run_all_parser.add_argument(
        "--config-dir", required=True, help="Directory containing YAML configs"
    )

    dag_parser = subparsers.add_parser(
        "generate-dag", help="Generate DAG stub from config"
    )
    dag_parser.add_argument("--config", required=True, help="Path to YAML config")

    show_parser = subparsers.add_parser(
        "show-table", help="Show table contents from DuckDB"
    )
    show_parser.add_argument("--table", required=True, help="Table name to inspect")

    return parser


def detect_destination_conflicts(configs: list) -> None:
    """
    Scan all configs for shared destination tables.
    Exits with code 1 and a clear message if any conflict is found.
    Call this before running any pipelines in run-all.
    """
    dest_map: dict[str, str] = {}
    conflicts: list[str] = []

    for cfg in configs:
        dest_key = f"{cfg.destination.schema}.{cfg.destination.table}"
        if dest_key in dest_map:
            conflicts.append(
                f"  '{cfg.pipeline_name}' and '{dest_map[dest_key]}' "
                f"both target {dest_key}"
            )
        else:
            dest_map[dest_key] = cfg.pipeline_name

    if conflicts:
        print("\nERROR: Duplicate destination tables detected:\n")
        for c in conflicts:
            print(c)
        print("\nFix your configs before running.\n")
        sys.exit(1)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    warehouse = DuckDBWarehouse(db_path="warehouse.duckdb")

    if args.command == "run":
        config = load_pipeline_config(args.config)
        executor = PipelineExecutor(warehouse=warehouse)
        result = executor.execute(config, force=getattr(args, "force", False))
        if result["status"] == "success":
            print(f"Pipeline '{config.pipeline_name}' executed successfully.")
        else:
            print(f"Pipeline '{config.pipeline_name}' failed:")
            print(result["error_message"])
            sys.exit(1)

    elif args.command == "run-all":
        yaml_files = sorted(Path(args.config_dir).glob("*.yaml"))
        if not yaml_files:
            print(f"No YAML files found in {args.config_dir}")
            return

        configs = [load_pipeline_config(str(p)) for p in yaml_files]

        # Task 8: check for conflicts before running any pipelines
        detect_destination_conflicts(configs)

        executor = PipelineExecutor(warehouse=warehouse)
        for config in configs:
            print(f"\n▶ Running: {config.pipeline_name}")
            result = executor.execute(config, force=False)
            if result["status"] == "success":
                print(f"  ✓ {config.pipeline_name}")
            else:
                print(f"  ✗ {config.pipeline_name}: {result['error_message']}")
                sys.exit(1)

    elif args.command == "generate-dag":
        config = load_pipeline_config(args.config)
        output_path = generate_dag_file(config)
        print(f"Generated DAG file: {output_path}")

    elif args.command == "show-table":
        print(warehouse.fetch_table(args.table).to_string(index=False))


if __name__ == "__main__":
    main()
