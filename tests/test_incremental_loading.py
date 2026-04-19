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


@pytest.fixture
def db(tmp_path):
    return DuckDBWarehouse(db_path=str(tmp_path / "test.duckdb"))


@pytest.fixture
def executor(db):
    return PipelineExecutor(warehouse=db)


def _make_config(
    tmp_path, name: str, csv_content: str, incremental_key: str = "created_at"
) -> PipelineConfig:
    csv_file = tmp_path / f"{name}.csv"
    csv_file.write_text(csv_content)
    return PipelineConfig(
        pipeline_name=name,
        owner="test",
        source=SourceConfig(
            type="csv",
            path=str(csv_file),
            incremental_key=incremental_key,
        ),
        destination=DestinationConfig(schema="raw", table=name),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
    )


# Watermark storage


def test_first_run_stores_watermark(tmp_path, db, executor):
    """After run 1 the max incremental_key value is stored in metadata_watermarks."""
    config = _make_config(
        tmp_path,
        "wm_test",
        "order_id,created_at\n1,2026-03-01\n2,2026-03-02\n3,2026-03-03\n",
    )
    executor.execute(config)

    watermark = db.get_watermark("wm_test", "created_at")
    assert watermark == "2026-03-03"


def test_second_run_updates_watermark(tmp_path, db, executor):
    """Watermark advances when a second run finds newer rows."""
    csv_path = tmp_path / "wm_update.csv"
    csv_path.write_text("order_id,created_at\n1,2026-03-01\n2,2026-03-02\n")

    config = PipelineConfig(
        pipeline_name="wm_update",
        owner="test",
        source=SourceConfig(
            type="csv", path=str(csv_path), incremental_key="created_at"
        ),
        destination=DestinationConfig(schema="raw", table="wm_update"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
    )
    executor.execute(config)

    # Add a newer row
    csv_path.write_text(
        "order_id,created_at\n1,2026-03-01\n2,2026-03-02\n3,2026-03-05\n"
    )
    executor.execute(config)

    assert db.get_watermark("wm_update", "created_at") == "2026-03-05"


# Row filtering


def test_second_run_loads_zero_rows_when_no_new_data(tmp_path, db, executor):
    """Second run with identical data loads 0 rows — all <= watermark."""
    config = _make_config(
        tmp_path,
        "filter_test",
        "order_id,created_at\n1,2026-03-01\n2,2026-03-02\n",
    )
    executor.execute(config)  # run 1 — loads 2, stores watermark
    result2 = executor.execute(config)  # run 2 — same data

    assert result2["rows_loaded"] == 0


def test_second_run_loads_only_rows_after_watermark(tmp_path, db, executor):
    """Only rows strictly after the watermark are loaded on run 2."""
    csv_path = tmp_path / "new_rows.csv"
    csv_path.write_text("order_id,created_at\n1,2026-03-01\n2,2026-03-02\n")

    config = PipelineConfig(
        pipeline_name="new_rows",
        owner="test",
        source=SourceConfig(
            type="csv", path=str(csv_path), incremental_key="created_at"
        ),
        destination=DestinationConfig(schema="raw", table="new_rows"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
    )
    executor.execute(config)  # loads 2 rows, watermark = 2026-03-02

    # Add exactly 1 new row after the watermark
    csv_path.write_text(
        "order_id,created_at\n1,2026-03-01\n2,2026-03-02\n3,2026-03-10\n"
    )
    result2 = executor.execute(config)

    assert result2["rows_loaded"] == 1


# Full load unchanged


def test_no_incremental_key_loads_all_rows_every_run(tmp_path, db, executor):
    """A config with incremental_key=None loads everything on every run."""
    csv_file = tmp_path / "full.csv"
    csv_file.write_text("order_id,created_at\n1,2026-03-01\n2,2026-03-02\n")

    config = PipelineConfig(
        pipeline_name="full_load",
        owner="test",
        source=SourceConfig(type="csv", path=str(csv_file), incremental_key=None),
        destination=DestinationConfig(schema="raw", table="full_load"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
    )

    result1 = executor.execute(config)
    result2 = executor.execute(config)

    assert result1["rows_loaded"] == 2
    assert result2["rows_loaded"] == 2


# Error handling


def test_invalid_incremental_key_column_returns_failed(tmp_path, db, executor):
    """A column name that does not exist in the CSV causes a FAILED run."""
    csv_file = tmp_path / "bad.csv"
    csv_file.write_text("order_id,amount\n1,100\n2,200\n")

    config = PipelineConfig(
        pipeline_name="bad_key",
        owner="test",
        source=SourceConfig(
            type="csv", path=str(csv_file), incremental_key="nonexistent_column"
        ),
        destination=DestinationConfig(schema="raw", table="bad_key"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
    )

    result = executor.execute(config)
    assert result["status"] == "failed"
    assert "nonexistent_column" in result["error_message"]
