from __future__ import annotations

from datetime import datetime, timezone

from pipeline_platform.config_parser import PipelineConfig
from pipeline_platform.warehouse.duckdb_client import DuckDBWarehouse


class PipelineRegistry:
    def __init__(self, warehouse: DuckDBWarehouse) -> None:
        self.warehouse = warehouse

    def ensure_metadata_tables(self) -> None:
        self.warehouse.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata_pipelines (
                pipeline_name VARCHAR PRIMARY KEY,
                owner VARCHAR,
                source_type VARCHAR,
                source_identifier VARCHAR,
                destination_schema VARCHAR,
                destination_table VARCHAR,
                schedule VARCHAR,
                load_mode VARCHAR,
                is_active BOOLEAN,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                last_run_at TIMESTAMP,
                last_status VARCHAR
            )
            """
        )
        self.warehouse.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata_pipeline_runs (
                run_id VARCHAR,
                pipeline_name VARCHAR,
                run_timestamp TIMESTAMP,
                status VARCHAR,
                rows_extracted BIGINT,
                rows_loaded BIGINT,
                execution_time_seconds DOUBLE,
                error_message VARCHAR
            )
            """
        )

    def register_pipeline(self, config: PipelineConfig) -> None:
        source_identifier = config.source.path or config.source.endpoint or "unknown"
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        self.warehouse.execute(
            f"""
            INSERT OR REPLACE INTO metadata_pipelines VALUES (
                '{config.pipeline_name}',
                '{config.owner}',
                '{config.source.type}',
                '{source_identifier}',
                '{config.destination.schema}',
                '{config.destination.table}',
                '{config.schedule.cron}',
                '{config.load_mode}',
                TRUE,
                COALESCE((SELECT created_at FROM metadata_pipelines WHERE pipeline_name = '{config.pipeline_name}'), TIMESTAMP '{now}'),
                TIMESTAMP '{now}',
                COALESCE((SELECT last_run_at FROM metadata_pipelines WHERE pipeline_name = '{config.pipeline_name}'), NULL),
                COALESCE((SELECT last_status FROM metadata_pipelines WHERE pipeline_name = '{config.pipeline_name}'), NULL)
            )
            """
        )

    def update_last_run_status(self, pipeline_name: str, status: str) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        self.warehouse.execute(
            f"""
            UPDATE metadata_pipelines
            SET last_run_at = TIMESTAMP '{now}',
                last_status = '{status}',
                updated_at = TIMESTAMP '{now}'
            WHERE pipeline_name = '{pipeline_name}'
            """
        )
