from __future__ import annotations

import pytest

from pipeline_platform.config_parser import (
    PipelineConfig,
    SourceConfig,
    DestinationConfig,
    ScheduleConfig,
    TransformConfig,
    QualityChecksConfig,
    NotifyConfig,
)
from pipeline_platform.metadata.registry import PipelineRegistry
from pipeline_platform.metadata.run_logger import RunLogger
from pipeline_platform.pipeline_generator import PipelineExecutor
from pipeline_platform.warehouse.duckdb_client import DuckDBWarehouse


@pytest.fixture
def db(tmp_path):
    return DuckDBWarehouse(db_path=str(tmp_path / "test.duckdb"))


@pytest.fixture
def registry(db):
    reg = PipelineRegistry(db)
    reg.ensure_metadata_tables()
    return reg, db


@pytest.fixture
def executor(db):
    return PipelineExecutor(warehouse=db)


def _make_config(tmp_path, name: str) -> PipelineConfig:
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id,value\n1,hello\n2,world\n")
    return PipelineConfig(
        pipeline_name=name,
        owner="test_owner",
        source=SourceConfig(type="csv", path=str(csv_file)),
        destination=DestinationConfig(schema="raw", table="test_table"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
        quality_checks=QualityChecksConfig(),
        notify=NotifyConfig(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# SQL INJECTION SAFETY
# ─────────────────────────────────────────────────────────────────────────────


def test_pipeline_name_with_single_quote_does_not_crash(tmp_path, executor, db):
    """A pipeline name containing a single quote must not break SQL execution."""
    config = _make_config(tmp_path, "o'reilly_pipeline")
    result = executor.execute(config)
    assert result["status"] == "success"


def test_pipeline_name_with_single_quote_is_stored_correctly(tmp_path, executor, db):
    """The pipeline name with a quote is stored and retrieved without corruption."""
    config = _make_config(tmp_path, "o'reilly_pipeline")
    executor.execute(config)

    rows = db.query(
        "SELECT pipeline_name FROM metadata_pipelines WHERE pipeline_name = ?",
        ["o'reilly_pipeline"],
    )
    assert len(rows) == 1
    assert rows[0]["pipeline_name"] == "o'reilly_pipeline"


def test_owner_with_single_quote_does_not_crash(tmp_path, db, registry):
    """An owner name containing a single quote must not break registration."""
    reg, db = registry
    config = PipelineConfig(
        pipeline_name="safe_pipe",
        owner="O'Brien Analytics",
        source=SourceConfig(type="csv", path="data.csv"),
        destination=DestinationConfig(schema="raw", table="safe_pipe"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
        quality_checks=QualityChecksConfig(),
        notify=NotifyConfig(),
    )
    reg.register_pipeline(config)

    rows = db.query(
        "SELECT owner FROM metadata_pipelines WHERE pipeline_name = ?",
        ["safe_pipe"],
    )
    assert rows[0]["owner"] == "O'Brien Analytics"


def test_error_message_with_single_quote_is_logged_correctly(tmp_path, db):
    """Error messages containing quotes are stored without corruption."""
    logger = RunLogger(db)
    db.execute("""
        CREATE TABLE IF NOT EXISTS metadata_pipeline_runs (
            run_id VARCHAR, pipeline_name VARCHAR, run_timestamp TIMESTAMP,
            status VARCHAR, rows_extracted BIGINT, rows_loaded BIGINT,
            execution_time_seconds DOUBLE, error_message VARCHAR
        )
    """)

    logger.log_run(
        run_id="test-run-1",
        pipeline_name="test_pipe",
        status="FAILED",
        rows_extracted=0,
        rows_loaded=0,
        execution_time_seconds=0.01,
        error_message="File 'data's_file.csv' not found",
    )

    rows = db.query(
        "SELECT error_message FROM metadata_pipeline_runs WHERE run_id = ?",
        ["test-run-1"],
    )
    assert rows[0]["error_message"] == "File 'data's_file.csv' not found"


def test_null_error_message_stored_as_null(tmp_path, db):
    """None error_message is stored as SQL NULL, not the string 'None'."""
    logger = RunLogger(db)
    db.execute("""
        CREATE TABLE IF NOT EXISTS metadata_pipeline_runs (
            run_id VARCHAR, pipeline_name VARCHAR, run_timestamp TIMESTAMP,
            status VARCHAR, rows_extracted BIGINT, rows_loaded BIGINT,
            execution_time_seconds DOUBLE, error_message VARCHAR
        )
    """)

    logger.log_run(
        run_id="test-run-2",
        pipeline_name="test_pipe",
        status="SUCCESS",
        rows_extracted=5,
        rows_loaded=5,
        execution_time_seconds=0.05,
        error_message=None,
    )

    rows = db.query(
        "SELECT error_message FROM metadata_pipeline_runs WHERE run_id = ?",
        ["test-run-2"],
    )
    assert rows[0]["error_message"] is None


def test_update_last_run_status_with_quoted_name(tmp_path, executor, db):
    """update_last_run_status works correctly with a quoted pipeline name."""
    config = _make_config(tmp_path, "pipe'with'quotes")
    executor.execute(config)

    rows = db.query(
        "SELECT last_status FROM metadata_pipelines WHERE pipeline_name = ?",
        ["pipe'with'quotes"],
    )
    assert rows[0]["last_status"] == "SUCCESS"


def test_run_log_recorded_for_quoted_pipeline_name(tmp_path, executor, db):
    """Run log entry is written correctly for a pipeline name with quotes."""
    config = _make_config(tmp_path, "quoted'pipe")
    executor.execute(config)

    runs = db.query(
        "SELECT status FROM metadata_pipeline_runs WHERE pipeline_name = ?",
        ["quoted'pipe"],
    )
    assert len(runs) == 1
    assert runs[0]["status"] == "SUCCESS"
