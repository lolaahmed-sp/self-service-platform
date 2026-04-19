from __future__ import annotations

from datetime import datetime, timezone

from pipeline_platform.warehouse.duckdb_client import DuckDBWarehouse


class RunLogger:
    def __init__(self, warehouse: DuckDBWarehouse) -> None:
        self.warehouse = warehouse

    def log_run(
        self,
        run_id: str,
        pipeline_name: str,
        status: str,
        rows_extracted: int,
        rows_loaded: int,
        execution_time_seconds: float,
        error_message: str | None,
    ) -> None:
        timestamp = datetime.now(timezone.utc).replace(tzinfo=None)
        self.warehouse.execute(
            """
            INSERT INTO metadata_pipeline_runs (
                run_id, pipeline_name, run_timestamp, status,
                rows_extracted, rows_loaded, execution_time_seconds,
                error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                pipeline_name,
                timestamp,
                status,
                rows_extracted,
                rows_loaded,
                execution_time_seconds,
                error_message,  # None becomes SQL NULL automatically
            ],
        )
