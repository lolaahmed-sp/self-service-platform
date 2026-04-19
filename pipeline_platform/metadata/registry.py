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
            """
            INSERT INTO metadata_pipelines (
                pipeline_name, owner, source_type, source_identifier,
                destination_schema, destination_table, schedule, load_mode,
                is_active, created_at, updated_at, last_run_at, last_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE, ?, ?, NULL, NULL)
            ON CONFLICT (pipeline_name) DO UPDATE SET
                owner              = excluded.owner,
                source_type        = excluded.source_type,
                source_identifier  = excluded.source_identifier,
                destination_schema = excluded.destination_schema,
                destination_table  = excluded.destination_table,
                schedule           = excluded.schedule,
                load_mode          = excluded.load_mode,
                is_active          = TRUE,
                updated_at         = excluded.updated_at
            """,
            [
                config.pipeline_name,
                config.owner,
                config.source.type,
                source_identifier,
                config.destination.schema,
                config.destination.table,
                config.schedule.cron,
                config.load_mode,
                now,  # created_at
                now,  # updated_at
            ],
        )

    def update_last_run_status(self, pipeline_name: str, status: str) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        self.warehouse.execute(
            """
            UPDATE metadata_pipelines
            SET last_run_at = ?,
                last_status = ?,
                updated_at  = ?
            WHERE pipeline_name = ?
            """,
            [now, status, now, pipeline_name],
        )
