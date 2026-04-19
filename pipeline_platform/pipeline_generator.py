from __future__ import annotations

import hashlib
import json
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
        Never raises — all failures are caught, logged, and returned as
        {"status": "failed", "error_message": "..."}.
        """
        self.registry.ensure_metadata_tables()
        self.registry.register_pipeline(config)

        # Config versioning (Task 12)
        config_hash, config_snapshot = self._config_hash(config)
        self.warehouse.track_config_version(
            config.pipeline_name, config_hash, config_snapshot
        )

        run_id = str(uuid.uuid4())
        start = time.time()
        rows_extracted = 0
        rows_loaded = 0

        try:
            # Extract
            dataframe = self._extract(config)
            rows_extracted = len(dataframe)

            # Data quality checks (Task 7)
            from pipeline_platform.quality.checker import run_quality_checks

            run_quality_checks(dataframe, config.quality_checks)

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

            # ── Load
            rows_loaded = self._load(config, dataframe)

            # Update watermark after successful load (Task 5)
            if config.source.incremental_key:
                self._update_watermark(config, dataframe)

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

            #  Failure notification (Task 10)
            if config.notify and config.notify.on_failure:
                from pipeline_platform.notifications.slack import (
                    send_failure_notification,
                )

                send_failure_notification(
                    webhook_url=config.notify.slack_webhook,
                    pipeline_name=config.pipeline_name,
                    error_message=str(exc),
                    run_id=run_id,
                )

            return {
                "status": "failed",
                "rows_extracted": rows_extracted,
                "rows_loaded": rows_loaded,
                "execution_time_seconds": duration,
                "error_message": str(exc),
            }

    # Extract

    def _extract(self, config: PipelineConfig) -> pd.DataFrame:
        if config.source.type == "csv":
            df = self.csv_ingestor.read(config.source.path)

            # Incremental filter (Task 5)
            incremental_key = config.source.incremental_key
            if incremental_key:
                if incremental_key not in df.columns:
                    raise ValueError(
                        f"incremental_key '{incremental_key}' not found "
                        f"in columns: {list(df.columns)}"
                    )
                watermark = self.warehouse.get_watermark(
                    config.pipeline_name, incremental_key
                )
                if watermark:
                    print(f"[Incremental] Filtering {incremental_key} > {watermark}")
                    df = df[df[incremental_key].astype(str) > watermark]
                print(f"[Incremental] {len(df)} rows after watermark filter")

            return df

        if config.source.type == "api":
            from pipeline_platform.sources.api_ingestor import APIIngestor

            ingestor = APIIngestor(
                endpoint=config.source.endpoint,
                auth_env_var=getattr(config.source, "auth_env_var", None),
            )
            return ingestor.extract()

        raise NotImplementedError(f"Unsupported source type: {config.source.type}")

    # Watermark update (Task 5)

    def _update_watermark(
        self, config: PipelineConfig, dataframe: pd.DataFrame
    ) -> None:
        key = config.source.incremental_key
        if not key or key not in dataframe.columns or dataframe.empty:
            return
        old_value = self.warehouse.get_watermark(config.pipeline_name, key)
        new_value = str(dataframe[key].max())
        self.warehouse.update_watermark(config.pipeline_name, key, new_value)
        print(f"[Watermark] {config.pipeline_name}.{key}: {old_value} → {new_value}")

    # Load

    def _load(self, config: PipelineConfig, dataframe: pd.DataFrame) -> int:
        table_name = self._physical_table_name(config)
        return self.warehouse.load_dataframe(
            table_name=table_name,
            dataframe=dataframe,
            load_mode=config.load_mode,
        )

    # Helpers

    @staticmethod
    def _physical_table_name(config: PipelineConfig) -> str:
        return f"{config.destination.schema}_{config.destination.table}"

    @staticmethod
    def _config_hash(config: PipelineConfig) -> tuple[str, str]:
        """
        Returns (hash, snapshot) for the current config state.
        The snapshot is a JSON string of the key config fields.
        The hash is a SHA-256 of that snapshot truncated to 16 chars.
        """
        snapshot = {
            "pipeline_name": config.pipeline_name,
            "owner": config.owner,
            "source_type": config.source.type,
            "source_path": config.source.path,
            "source_endpoint": config.source.endpoint,
            "incremental_key": config.source.incremental_key,
            "destination_schema": config.destination.schema,
            "destination_table": config.destination.table,
            "schedule_cron": config.schedule.cron,
            "load_mode": config.load_mode,
        }
        snapshot_str = json.dumps(snapshot, sort_keys=True)
        config_hash = hashlib.sha256(snapshot_str.encode()).hexdigest()[:16]
        return config_hash, snapshot_str


# DAG generation


def generate_dag_file(config: PipelineConfig) -> str:
    output_dir = Path("generated_dags")
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / f"{config.pipeline_name}_dag.py"
    output_path.write_text(render_dag_file(config), encoding="utf-8")
    return str(output_path)
