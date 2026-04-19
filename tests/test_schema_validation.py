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


def _make_config(
    tmp_path,
    name: str,
    csv_content: str,
    load_mode: str = "overwrite",
) -> PipelineConfig:
    """Write a CSV and return the config pointing at it."""
    csv_file = tmp_path / f"{name}.csv"
    csv_file.write_text(csv_content)
    return PipelineConfig(
        pipeline_name=name,
        owner="test",
        source=SourceConfig(type="csv", path=str(csv_file)),
        destination=DestinationConfig(schema="raw", table=name),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode=load_mode,
        transform=TransformConfig(),
    )


# Matches the real customers.csv structure confirmed from metadata_schemas:
# customer_id, full_name, country, created_at — all object dtype
CUSTOMERS_CSV = (
    "customer_id,full_name,country,created_at\n"
    "C001,Ada Ibrahim,Nigeria,2026-02-20T08:00:00\n"
    "C002,Tomi Adebayo,Nigeria,2026-02-21T09:15:00\n"
)


# SCHEMA STORAGE


def test_first_run_stores_schema(tmp_path, db, executor):
    """After run 1 all column names are stored in metadata_schemas."""
    config = _make_config(tmp_path, "schema_store", CUSTOMERS_CSV)
    executor.execute(config)

    stored = db.get_stored_schema("schema_store")

    assert "customer_id" in stored
    assert "full_name" in stored
    assert "country" in stored
    assert "created_at" in stored


def test_first_run_stores_correct_types(tmp_path, db, executor):
    """Column types stored match what pandas infers from the CSV."""
    config = _make_config(tmp_path, "type_store", CUSTOMERS_CSV)
    executor.execute(config)

    stored = db.get_stored_schema("type_store")

    # All four columns in customers.csv are string/object type
    assert stored["customer_id"] == "object"
    assert stored["full_name"] == "object"
    assert stored["country"] == "object"
    assert stored["created_at"] == "object"


def test_second_run_same_schema_succeeds(tmp_path, db, executor):
    """A second run with identical schema must succeed — not raise."""
    config = _make_config(tmp_path, "same_schema", CUSTOMERS_CSV)

    result1 = executor.execute(config)
    result2 = executor.execute(config)

    assert result1["status"] == "success"
    assert result2["status"] == "success"


# DRIFT DETECTION - column removed


def test_removed_column_detected_as_drift(tmp_path, db, executor):
    """Removing a column from the source CSV causes a FAILED run."""
    # Run 1 — store schema with 4 columns
    config_v1 = _make_config(tmp_path, "removed_col", CUSTOMERS_CSV)
    executor.execute(config_v1)

    # Run 2 — CSV missing the 'country' column
    csv_v2 = tmp_path / "removed_col.csv"
    csv_v2.write_text(
        "customer_id,full_name,created_at\nC003,Emeka Obi,2026-03-01T10:00:00\n"
    )
    config_v2 = PipelineConfig(
        pipeline_name="removed_col",
        owner="test",
        source=SourceConfig(type="csv", path=str(csv_v2)),
        destination=DestinationConfig(schema="raw", table="removed_col"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="overwrite",
        transform=TransformConfig(),
    )

    result = executor.execute(config_v2, force=False)

    assert result["status"] == "failed"
    assert "country" in result["error_message"]


# DRIFT DETECTION - column added


def test_added_column_detected_as_drift(tmp_path, db, executor):
    """Adding a new column to the source CSV causes a FAILED run."""
    # Run 1 — store schema with 4 columns
    config_v1 = _make_config(tmp_path, "added_col", CUSTOMERS_CSV)
    executor.execute(config_v1)

    # Run 2 — CSV has an extra 'email' column
    csv_v2 = tmp_path / "added_col.csv"
    csv_v2.write_text(
        "customer_id,full_name,country,created_at,email\n"
        "C003,Emeka Obi,Nigeria,2026-03-01T10:00:00,emeka@example.com\n"
    )
    config_v2 = PipelineConfig(
        pipeline_name="added_col",
        owner="test",
        source=SourceConfig(type="csv", path=str(csv_v2)),
        destination=DestinationConfig(schema="raw", table="added_col"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="overwrite",
        transform=TransformConfig(),
    )

    result = executor.execute(config_v2, force=False)

    assert result["status"] == "failed"
    assert "email" in result["error_message"]


# DRIFT DETECTION - error message content


def test_drift_error_message_mentions_schema_drift(tmp_path, db, executor):
    """The error message must contain 'drift' so operators know what failed."""
    config_v1 = _make_config(tmp_path, "drift_msg", CUSTOMERS_CSV)
    executor.execute(config_v1)

    csv_v2 = tmp_path / "drift_msg.csv"
    csv_v2.write_text(
        "customer_id,full_name,created_at\nC003,Emeka Obi,2026-03-01T10:00:00\n"
    )
    config_v2 = PipelineConfig(
        pipeline_name="drift_msg",
        owner="test",
        source=SourceConfig(type="csv", path=str(csv_v2)),
        destination=DestinationConfig(schema="raw", table="drift_msg"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="overwrite",
        transform=TransformConfig(),
    )

    result = executor.execute(config_v2, force=False)

    assert result["status"] == "failed"
    assert "drift" in result["error_message"].lower()


def test_drift_run_is_logged_as_failed_in_run_table(tmp_path, db, executor):
    """Schema drift must be recorded in metadata_pipeline_runs as FAILED."""
    config_v1 = _make_config(tmp_path, "drift_log", CUSTOMERS_CSV)
    executor.execute(config_v1)

    csv_v2 = tmp_path / "drift_log.csv"
    csv_v2.write_text(
        "customer_id,full_name,created_at\nC003,Emeka Obi,2026-03-01T10:00:00\n"
    )
    config_v2 = PipelineConfig(
        pipeline_name="drift_log",
        owner="test",
        source=SourceConfig(type="csv", path=str(csv_v2)),
        destination=DestinationConfig(schema="raw", table="drift_log"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="overwrite",
        transform=TransformConfig(),
    )
    executor.execute(config_v2, force=False)

    runs = db.query(
        "SELECT status FROM metadata_pipeline_runs "
        "WHERE pipeline_name = ? ORDER BY run_timestamp DESC LIMIT 1",
        ["drift_log"],
    )
    assert runs[0]["status"] == "FAILED"


# FORCE FLAG — bypass schema check


def test_force_flag_bypasses_drift_and_succeeds(tmp_path, db, executor):
    """force=True allows a run to proceed even when schema drift is detected."""
    config_v1 = _make_config(tmp_path, "force_bypass", CUSTOMERS_CSV)
    executor.execute(config_v1)

    csv_v2 = tmp_path / "force_bypass.csv"
    csv_v2.write_text(
        "customer_id,full_name,created_at\nC003,Emeka Obi,2026-03-01T10:00:00\n"
    )
    config_v2 = PipelineConfig(
        pipeline_name="force_bypass",
        owner="test",
        source=SourceConfig(type="csv", path=str(csv_v2)),
        destination=DestinationConfig(schema="raw", table="force_bypass"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
    )

    result = executor.execute(config_v2, force=True)

    assert result["status"] == "success"


def test_force_flag_updates_stored_schema(tmp_path, db, executor):
    """After a force run the stored schema reflects the new column set."""
    config_v1 = _make_config(tmp_path, "force_update", CUSTOMERS_CSV)
    executor.execute(config_v1)

    # New schema — 'country' removed, 'email' added
    csv_v2 = tmp_path / "force_update.csv"
    csv_v2.write_text(
        "customer_id,full_name,created_at,email\n"
        "C003,Emeka Obi,2026-03-01T10:00:00,emeka@example.com\n"
    )
    config_v2 = PipelineConfig(
        pipeline_name="force_update",
        owner="test",
        source=SourceConfig(type="csv", path=str(csv_v2)),
        destination=DestinationConfig(schema="raw", table="force_update"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="overwrite",
        transform=TransformConfig(),
    )
    executor.execute(config_v2, force=True)

    stored = db.get_stored_schema("force_update")

    # New schema should be stored after force run
    assert "email" in stored
    assert "customer_id" in stored


# FIRST RUN — no schema stored yet


def test_first_run_has_no_stored_schema_before_execute(tmp_path, db):
    """Before any run, get_stored_schema returns an empty dict."""
    stored = db.get_stored_schema("brand_new_pipeline")
    assert stored == {}


def test_compare_schema_returns_empty_on_first_run(tmp_path, db):
    """compare_schema returns no errors when no schema is stored yet."""
    import pandas as pd

    df = pd.DataFrame({"customer_id": ["C001"], "full_name": ["Ada"]})
    drift = db.compare_schema("never_run_pipeline", df)
    assert drift == []
