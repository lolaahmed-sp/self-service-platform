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

    subparsers.add_parser(
        "status",
        help="Show a health summary of all registered pipelines",
    )

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


def _print_status(warehouse: DuckDBWarehouse) -> None:
    """Print a human-readable health summary of all registered pipelines."""
    try:
        pipelines = warehouse.query(
            "SELECT * FROM metadata_pipelines ORDER BY pipeline_name"
        )
    except Exception:
        print("No pipelines registered yet. Run a pipeline first.")
        return

    if not pipelines:
        print("No pipelines registered yet. Run a pipeline first.")
        return

    print(f"\n{'=' * 60}")
    print(f"  PIPELINE HEALTH SUMMARY  ({len(pipelines)} pipeline(s))")
    print(f"{'=' * 60}")

    for row in pipelines:
        name = row["pipeline_name"]

        # Total runs and failure count
        try:
            run_stats = warehouse.query(
                """
                SELECT
                    COUNT(*) AS total_runs,
                    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failures
                FROM metadata_pipeline_runs
                WHERE pipeline_name = ?
                """,
                [name],
            )
            total = int(run_stats[0]["total_runs"]) if run_stats else 0
            failures = int(run_stats[0]["failures"] or 0) if run_stats else 0
        except Exception:
            total = 0
            failures = 0

        # Last error message if most recent run was FAILED
        last_error = None
        try:
            error_rows = warehouse.query(
                """
                SELECT error_message
                FROM metadata_pipeline_runs
                WHERE pipeline_name = ?
                  AND status = 'FAILED'
                ORDER BY run_timestamp DESC
                LIMIT 1
                """,
                [name],
            )
            if error_rows and error_rows[0]["error_message"]:
                last_error = str(error_rows[0]["error_message"])[:120]
        except Exception:
            pass

        # Status indicator
        last_status = row.get("last_status") or "NEVER RUN"
        indicator = (
            "✓" if last_status == "SUCCESS" else "✗" if last_status == "FAILED" else "—"
        )

        print(f"\n  {indicator}  {name}")
        print(f"     Owner    : {row.get('owner', '—')}")
        print(f"     Schedule : {row.get('schedule', '—')}")
        print(f"     Load mode: {row.get('load_mode', '—')}")
        print(f"     Last run : {row.get('last_run_at', 'Never')}")
        print(f"     Status   : {last_status}")
        print(f"     Runs     : {total} total  |  {failures} failed")

        if last_error:
            print(f"     Last err : {last_error}")

    print(f"\n{'=' * 60}\n")


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

    elif args.command == "status":
        _print_status(warehouse)


if __name__ == "__main__":
    main()
