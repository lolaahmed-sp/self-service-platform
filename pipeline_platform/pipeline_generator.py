from __future__ import annotations

import time
import uuid
from pathlib import Path

import pandas as pd

from pipeline_platform.config_parser import PipelineConfig
from pipeline_platform.errors import SchemaValidationError
from pipeline_platform.metadata.registry import PipelineRegistry
from pipeline_platform.metadata.run_logger import RunLogger
from pipeline_platform.orchestration.dag_template import render_dag_file
from pipeline_platform.sources.csv_ingestor import CSVIngestor
from pipeline_platform.warehouse.duckdb_client import DuckDBWarehouse


class PipelineExecutor:
    def __init__(self, warehouse: DuckDBWarehouse) -> None:
        self.warehouse = warehouse
        self.registry = PipelineRegistry(warehouse)
        self.run_logger = RunLogger(warehouse)
        self.csv_ingestor = CSVIngestor()

    def execute(
        self,
        config: PipelineConfig,
        force: bool = False,
    ) -> dict:
        """
        Run a full pipeline and return a result dict.
        Never raises — failures are caught, logged, and returned as
        {"status": "failed", "error_message": "..."}.
        """
        self.registry.ensure_metadata_tables()
        self.registry.register_pipeline(config)

        run_id = str(uuid.uuid4())
        start = time.time()
        rows_extracted = 0
        rows_loaded = 0

        try:
            # Extract
            dataframe = self._extract(config)
            rows_extracted = len(dataframe)

            # Schema validation (Task 6)
            if not force:
                drift = self.warehouse.compare_schema(config.pipeline_name, dataframe)
                if drift:
                    raise SchemaValidationError(
                        "Schema drift detected:\n"
                        + "\n".join(f"  - {d}" for d in drift)
                    )

            # Store schema after validation passes
            self.warehouse.store_schema(config.pipeline_name, dataframe)

            # Load
            rows_loaded = self._load(config, dataframe)

            # Transform (Task 2)
            if config.transform and config.transform.sql:
                print(f"[Transform] Running: {config.transform.sql}")
                self.warehouse.execute_sql_file(config.transform.sql)

            duration = round(time.time() - start, 2)

            self.run_logger.log_run(
                run_id=run_id,
                pipeline_name=config.pipeline_name,
                status="SUCCESS",
                rows_extracted=rows_extracted,
                rows_loaded=rows_loaded,
                execution_time_seconds=duration,
                error_message=None,
            )

            self.registry.update_last_run_status(config.pipeline_name, "SUCCESS")
            return {
                "status": "success",
                "rows_extracted": rows_extracted,
                "rows_loaded": rows_loaded,
                "execution_time_seconds": duration,
                "error_message": None,
            }

        except Exception as exc:  # noqa: BLE001
            duration = round(time.time() - start, 2)

            self.run_logger.log_run(
                run_id=run_id,
                pipeline_name=config.pipeline_name,
                status="FAILED",
                rows_extracted=rows_extracted,
                rows_loaded=rows_loaded,
                execution_time_seconds=duration,
                error_message=str(exc),
            )
            self.registry.update_last_run_status(config.pipeline_name, "FAILED")

            # Return failure dict - do NOT re-raise, since this is meant to be called from a CLI where we want to catch and print errors.
            return {
                "status": "failed",
                "rows_extracted": rows_extracted,
                "rows_loaded": rows_loaded,
                "execution_time_seconds": duration,
                "error_message": str(exc),
            }

    def _extract(self, config: PipelineConfig) -> pd.DataFrame:
        if config.source.type == "csv":
            return self.csv_ingestor.read(config.source.path)
        raise NotImplementedError(f"Unsupported source type: {config.source.type}")

    def _load(self, config: PipelineConfig, dataframe: pd.DataFrame) -> int:
        table_name = self._physical_table_name(config)
        return self.warehouse.load_dataframe(
            table_name=table_name,
            dataframe=dataframe,
            load_mode=config.load_mode,
        )

    @staticmethod
    def _physical_table_name(config: PipelineConfig) -> str:
        return f"{config.destination.schema}_{config.destination.table}"


def generate_dag_file(config: PipelineConfig) -> str:
    output_dir = Path("generated_dags")
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / f"{config.pipeline_name}_dag.py"
    output_path.write_text(render_dag_file(config), encoding="utf-8")
    return str(output_path)
