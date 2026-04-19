from __future__ import annotations

import pytest

from pipeline_platform.config_parser import (
    PipelineConfig,
    SourceConfig,
    DestinationConfig,
    ScheduleConfig,
    TransformConfig,
)
from pipeline_platform.pipeline_generator import PipelineExecutor
from pipeline_platform.warehouse.duckdb_client import DuckDBWarehouse


# FIXTURES


@pytest.fixture
def db(tmp_path):
    """Isolated DuckDB — never touches the real warehouse.duckdb."""
    return DuckDBWarehouse(db_path=str(tmp_path / "test.duckdb"))


@pytest.fixture
def executor(db):
    return PipelineExecutor(warehouse=db)


# HELPER
# Builds PipelineConfig backed by temp CSV file.
# Uses actual dataclasses.


def _make_config(tmp_path, name: str = "test_orders", load_mode: str = "append"):
    csv_file = tmp_path / f"{name}.csv"
    csv_file.write_text("order_id,amount\n1,100\n2,200\n")

    return PipelineConfig(
        pipeline_name=name,
        owner="test_team",
        source=SourceConfig(
            type="csv",
            path=str(csv_file),
        ),
        destination=DestinationConfig(schema="raw", table=name),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode=load_mode,
        transform=TransformConfig(),
    )


# SUCCESS PATH


def test_execute_returns_success_status(tmp_path, executor):
    result = executor.execute(_make_config(tmp_path))
    assert result["status"] == "success"


def test_execute_returns_correct_row_counts(tmp_path, executor):
    """rows_extracted and rows_loaded must both match the CSV row count."""
    result = executor.execute(_make_config(tmp_path))
    assert result["rows_extracted"] == 2
    assert result["rows_loaded"] == 2


def test_execute_data_lands_in_warehouse(tmp_path, db, executor):
    """Data must be queryable from DuckDB after a successful run."""
    executor.execute(_make_config(tmp_path, name="landing_test"))

    # _physical_table_name joins schema + table with underscore
    rows = db.query("SELECT * FROM raw_landing_test")
    assert len(rows) == 2


def test_execute_logs_run_record_with_success_status(tmp_path, db, executor):
    """A successful run writes one record to metadata_pipeline_runs."""
    executor.execute(_make_config(tmp_path, name="log_test"))

    runs = db.query(
        "SELECT * FROM metadata_pipeline_runs WHERE pipeline_name = ?",
        ["log_test"],
    )
    assert len(runs) == 1
    assert runs[0]["status"] == "SUCCESS"
    assert runs[0]["rows_loaded"] == 2
    assert runs[0]["error_message"] is None


def test_execute_registers_pipeline_in_registry(tmp_path, db, executor):
    """Every run must register the pipeline in metadata_pipelines."""
    executor.execute(_make_config(tmp_path, name="reg_test"))

    rows = db.query(
        "SELECT last_status FROM metadata_pipelines WHERE pipeline_name = ?",
        ["reg_test"],
    )
    assert len(rows) == 1
    assert rows[0]["last_status"] == "SUCCESS"


def test_execute_records_non_negative_execution_time(tmp_path, executor):
    result = executor.execute(_make_config(tmp_path))
    assert result["execution_time_seconds"] >= 0


def test_execute_append_mode_accumulates_rows(tmp_path, db, executor):
    """Running append mode twice doubles the row count."""
    config = _make_config(tmp_path, name="append_test", load_mode="append")

    executor.execute(config)
    executor.execute(config)

    rows = db.query("SELECT * FROM raw_append_test")
    assert len(rows) == 4  # 2 rows × 2 runs


def test_execute_overwrite_mode_replaces_rows(tmp_path, db, executor):
    """Running overwrite mode twice keeps the original row count."""
    config = _make_config(tmp_path, name="overwrite_test", load_mode="overwrite")

    executor.execute(config)
    executor.execute(config)

    rows = db.query("SELECT * FROM raw_overwrite_test")
    assert len(rows) == 2  # not doubled


# FAILURE PATH


def test_execute_logs_failed_run_on_missing_csv(tmp_path, db, executor):
    """A missing source file is caught, logged as FAILED, and returned cleanly."""
    config = PipelineConfig(
        pipeline_name="bad_source",
        owner="me",
        source=SourceConfig(
            type="csv",
            path=str(tmp_path / "nonexistent.csv"),  # does not exist
        ),
        destination=DestinationConfig(schema="raw", table="bad_source"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
    )

    result = executor.execute(config)

    assert result["status"] == "failed"
    assert result["error_message"] is not None

    runs = db.query(
        "SELECT status, error_message FROM metadata_pipeline_runs "
        "WHERE pipeline_name = ?",
        ["bad_source"],
    )
    assert len(runs) == 1
    assert runs[0]["status"] == "FAILED"
    assert runs[0]["error_message"] is not None


def test_execute_failed_run_never_raises(tmp_path, executor):
    """The executor always returns a result dict — never raises to the caller."""
    config = PipelineConfig(
        pipeline_name="quiet_fail",
        owner="me",
        source=SourceConfig(
            type="csv",
            path=str(tmp_path / "missing.csv"),
        ),
        destination=DestinationConfig(schema="raw", table="quiet_fail"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
    )

    result = executor.execute(config)
    assert result["status"] == "failed"


# SCHEMA VALIDATION (Task 6)


def test_execute_force_flag_bypasses_schema_drift(tmp_path, db, executor):
    """
    force=True bypasses the schema drift CHECK so the run proceeds.
    Run 1 stores a 2-column schema.
    Run 2 has a different column set — without force it would be FAILED,
    but with force=True it must return 'success'.
    Uses append mode so DuckDB does not reject the structural mismatch.
    """
    # Run 1 — stores schema with 2 columns
    csv_v1 = tmp_path / "schema_v1.csv"
    csv_v1.write_text("id,name\n1,Alice\n")
    config_v1 = PipelineConfig(
        pipeline_name="schema_force_test",
        owner="me",
        source=SourceConfig(type="csv", path=str(csv_v1)),
        destination=DestinationConfig(schema="raw", table="schema_force_v1"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
    )
    result1 = executor.execute(config_v1)
    assert result1["status"] == "success"

    # Run 2 — same pipeline name (so stored schema is found), different CSV shape,
    # force=True must bypass the drift check and succeed
    csv_v2 = tmp_path / "schema_v2.csv"
    csv_v2.write_text("id,name,new_col\n2,Bob,extra\n")
    config_v2 = PipelineConfig(
        pipeline_name="schema_force_test",
        owner="me",
        source=SourceConfig(type="csv", path=str(csv_v2)),
        destination=DestinationConfig(schema="raw", table="schema_force_v2"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
    )

    result2 = executor.execute(config_v2, force=True)
    assert result2["status"] == "success"


def test_execute_schema_drift_without_force_logs_failure(tmp_path, db, executor):
    """
    Schema drift without force=True must result in a FAILED run record,
    not a crash.
    """
    # Run 1 — stores schema
    csv_v1 = tmp_path / "drift_v1.csv"
    csv_v1.write_text("id,name\n1,Alice\n")
    config_v1 = PipelineConfig(
        pipeline_name="drift_test",
        owner="me",
        source=SourceConfig(type="csv", path=str(csv_v1)),
        destination=DestinationConfig(schema="raw", table="drift_test"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="overwrite",
        transform=TransformConfig(),
    )
    executor.execute(config_v1)

    # Run 2 — different columns, no force flag
    csv_v2 = tmp_path / "drift_v2.csv"
    csv_v2.write_text("id,name,extra\n2,Bob,x\n")
    config_v2 = PipelineConfig(
        pipeline_name="drift_test",
        owner="me",
        source=SourceConfig(type="csv", path=str(csv_v2)),
        destination=DestinationConfig(schema="raw", table="drift_test"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="overwrite",
        transform=TransformConfig(),
    )

    result = executor.execute(config_v2, force=False)
    assert result["status"] == "failed"
    assert "drift" in result["error_message"].lower()


# DUPLICATE DESTINATION DETECTION (Task 8)


def test_detect_conflicts_raises_on_duplicate_destination():
    """Two configs sharing schema.table trigger sys.exit before any pipeline runs."""
    from main import detect_destination_conflicts

    configs = [
        PipelineConfig(
            pipeline_name="pipe_one",
            owner="me",
            source=SourceConfig(type="csv", path="a.csv"),
            destination=DestinationConfig(schema="raw", table="shared"),
            schedule=ScheduleConfig(cron="0 8 * * *"),
            load_mode="append",
            transform=TransformConfig(),
        ),
        PipelineConfig(
            pipeline_name="pipe_two",
            owner="me",
            source=SourceConfig(type="csv", path="b.csv"),
            destination=DestinationConfig(schema="raw", table="shared"),
            schedule=ScheduleConfig(cron="0 9 * * *"),
            load_mode="append",
            transform=TransformConfig(),
        ),
    ]

    with pytest.raises(SystemExit):
        detect_destination_conflicts(configs)


def test_detect_conflicts_passes_on_unique_destinations():
    """Two configs with different destinations must not raise."""
    from main import detect_destination_conflicts

    configs = [
        PipelineConfig(
            pipeline_name="pipe_one",
            owner="me",
            source=SourceConfig(type="csv", path="a.csv"),
            destination=DestinationConfig(schema="raw", table="customers"),
            schedule=ScheduleConfig(cron="0 8 * * *"),
            load_mode="append",
            transform=TransformConfig(),
        ),
        PipelineConfig(
            pipeline_name="pipe_two",
            owner="me",
            source=SourceConfig(type="csv", path="b.csv"),
            destination=DestinationConfig(schema="raw", table="orders"),
            schedule=ScheduleConfig(cron="0 9 * * *"),
            load_mode="append",
            transform=TransformConfig(),
        ),
    ]

    detect_destination_conflicts(configs)  # should not raise
