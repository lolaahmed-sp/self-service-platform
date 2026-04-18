from __future__ import annotations

import time
import uuid
from pathlib import Path

import pandas as pd

from pipeline_platform.config_parser import PipelineConfig
from pipeline_platform.metadata.registry import PipelineRegistry
from pipeline_platform.metadata.run_logger import RunLogger
from pipeline_platform.orchestration.dag_template import render_dag_file
from pipeline_platform.sources.csv_ingestor import CSVIngestor
from pipeline_platform.warehouse.duckdb_client import DuckDBWarehouse
from pipeline_platform.errors import SchemaValidationError


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
        # FIX: removed unused 'dataframe' parameter — _extract() is always called
    ) -> dict:
        self.registry.ensure_metadata_tables()
        self.registry.register_pipeline(config)

        run_id = str(uuid.uuid4())
        start = time.time()
        rows_extracted = 0
        rows_loaded = 0

        try:
            # Step 1: Extract (with incremental support)
            dataframe = self._extract(config)
            rows_extracted = len(dataframe)

            # Step 2: Schema Validation (Task 6)
            if not force:
                drift = self.warehouse.compare_schema(config.pipeline_name, dataframe)
                if drift:
                    raise SchemaValidationError(
                        "Schema drift detected:\n"
                        + "\n".join(f"  - {d}" for d in drift)
                    )

            # Always store schema after validation passes
            self.warehouse.store_schema(config.pipeline_name, dataframe)

            # Step 3: Load
            rows_loaded = self._load(config, dataframe)

            # Step 4: Update Watermark (Task 5)
            if config.source.incremental_key and not dataframe.empty:
                self._update_watermark(config, dataframe)

            # Step 5: Transform (Task 2)
            if config.transform and config.transform.enabled and config.transform.sql:
                print(f"[Transform] Running SQL file: {config.transform.sql}")
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

            return {
                "status": "failed",
                "rows_extracted": rows_extracted,
                "rows_loaded": rows_loaded,
                "execution_time_seconds": duration,
                "error_message": str(exc),
            }

    # =========================
    # EXTRACTION
    # =========================

    def _extract(self, config: PipelineConfig) -> pd.DataFrame:
        incremental_key = config.source.incremental_key

        watermark = None
        if incremental_key:
            watermark = self.warehouse.get_watermark(
                config.pipeline_name, incremental_key
            )
            print(f"[Extract] Watermark = {watermark}")

        if config.source.type == "csv":
            df = self.csv_ingestor.read(config.source.path)

            if incremental_key and watermark:
                if incremental_key not in df.columns:
                    raise ValueError(
                        f"Incremental key '{incremental_key}' not in CSV columns"
                    )
                df = df[df[incremental_key].astype(str) > watermark]

            return df

        elif config.source.type == "api":
            from pipeline_platform.sources.api_ingestor import APIIngestor

            ingestor = APIIngestor(
                endpoint=config.source.endpoint,
                auth_env_var=getattr(config.source, "auth_env_var", None),
            )
            return ingestor.extract(
                incremental_key=incremental_key,
                watermark=watermark,
            )

        raise NotImplementedError(f"Unsupported source type: {config.source.type}")

    # =========================
    # LOAD
    # =========================

    def _load(self, config: PipelineConfig, dataframe: pd.DataFrame) -> int:
        table_name = self._physical_table_name(config)

        rows_loaded = self.warehouse.load_dataframe(
            table_name=table_name,
            dataframe=dataframe,
            load_mode=config.load_mode,
        )

        print(f"[Load] {rows_loaded} rows → {table_name} ({config.load_mode})")
        return rows_loaded

    # =========================
    # WATERMARK
    # =========================

    def _update_watermark(
        self, config: PipelineConfig, dataframe: pd.DataFrame
    ) -> None:
        key = config.source.incremental_key

        if key not in dataframe.columns:
            raise ValueError(
                f"Incremental key '{key}' not found in dataset columns: "
                f"{list(dataframe.columns)}"
            )

        old_value = self.warehouse.get_watermark(config.pipeline_name, key)
        new_value = str(dataframe[key].max())

        self.warehouse.update_watermark(
            pipeline_name=config.pipeline_name,
            key=key,
            value=new_value,
        )

        print(f"[Watermark] {config.pipeline_name}.{key}: {old_value} → {new_value}")

    # =========================
    # HELPERS
    # =========================

    @staticmethod
    def _physical_table_name(config: PipelineConfig) -> str:
        return f"{config.destination.schema}_{config.destination.table}"


# =========================
# DAG GENERATION
# =========================


def generate_dag_file(config: PipelineConfig) -> str:
    output_dir = Path("generated_dags")
    output_dir.mkdir(exist_ok=True)

    output_path = output_dir / f"{config.pipeline_name}_dag.py"
    output_path.write_text(render_dag_file(config), encoding="utf-8")

    return str(output_path)
