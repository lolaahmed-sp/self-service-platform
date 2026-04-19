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
from pipeline_platform.pipeline_generator import PipelineExecutor
from pipeline_platform.warehouse.duckdb_client import DuckDBWarehouse


@pytest.fixture
def db(tmp_path):
    return DuckDBWarehouse(db_path=str(tmp_path / "test.duckdb"))


@pytest.fixture
def executor(db):
    return PipelineExecutor(warehouse=db)


def _make_config(
    tmp_path, name: str = "test_pipe", cron: str = "0 8 * * *"
) -> PipelineConfig:
    csv_file = tmp_path / f"{name}.csv"
    csv_file.write_text("id,value\n1,hello\n2,world\n")
    return PipelineConfig(
        pipeline_name=name,
        owner="test_owner",
        source=SourceConfig(type="csv", path=str(csv_file)),
        destination=DestinationConfig(schema="raw", table=name),
        schedule=ScheduleConfig(cron=cron),
        load_mode="append",
        transform=TransformConfig(),
        quality_checks=QualityChecksConfig(),
        notify=NotifyConfig(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG HISTORY TABLE
# ─────────────────────────────────────────────────────────────────────────────


def test_first_run_creates_config_history_record(tmp_path, db, executor):
    """First pipeline run writes one record to metadata_config_history."""
    executor.execute(_make_config(tmp_path))

    rows = db.query(
        "SELECT * FROM metadata_config_history WHERE pipeline_name = ?",
        ["test_pipe"],
    )
    assert len(rows) == 1


def test_config_history_stores_pipeline_name(tmp_path, db, executor):
    """The pipeline name is stored correctly in config history."""
    executor.execute(_make_config(tmp_path, name="my_pipeline"))

    rows = db.query(
        "SELECT pipeline_name FROM metadata_config_history WHERE pipeline_name = ?",
        ["my_pipeline"],
    )
    assert rows[0]["pipeline_name"] == "my_pipeline"


def test_config_history_stores_hash(tmp_path, db, executor):
    """A non-empty config hash is stored."""
    executor.execute(_make_config(tmp_path))

    rows = db.query(
        "SELECT config_hash FROM metadata_config_history WHERE pipeline_name = ?",
        ["test_pipe"],
    )
    assert rows[0]["config_hash"] is not None
    assert len(rows[0]["config_hash"]) == 16  # SHA-256 truncated to 16 chars


def test_config_history_stores_snapshot(tmp_path, db, executor):
    """A JSON snapshot of the config is stored."""
    executor.execute(_make_config(tmp_path))

    rows = db.query(
        "SELECT config_snapshot FROM metadata_config_history WHERE pipeline_name = ?",
        ["test_pipe"],
    )
    import json

    snapshot = json.loads(rows[0]["config_snapshot"])
    assert snapshot["pipeline_name"] == "test_pipe"
    assert snapshot["load_mode"] == "append"


# ─────────────────────────────────────────────────────────────────────────────
# CHANGE DETECTION
# ─────────────────────────────────────────────────────────────────────────────


def test_second_run_same_config_does_not_add_new_record(tmp_path, db, executor):
    """Running the same config twice creates only one history record."""
    config = _make_config(tmp_path)
    executor.execute(config)
    executor.execute(config)

    rows = db.query(
        "SELECT * FROM metadata_config_history WHERE pipeline_name = ?",
        ["test_pipe"],
    )
    assert len(rows) == 1


def test_changed_config_creates_new_history_record(tmp_path, db, executor):
    """Changing the schedule creates a second history record."""
    csv_file = tmp_path / "test_pipe.csv"
    csv_file.write_text("id,value\n1,hello\n")

    config_v1 = PipelineConfig(
        pipeline_name="test_pipe",
        owner="test_owner",
        source=SourceConfig(type="csv", path=str(csv_file)),
        destination=DestinationConfig(schema="raw", table="test_pipe"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
        quality_checks=QualityChecksConfig(),
        notify=NotifyConfig(),
    )
    executor.execute(config_v1)

    # Change the schedule
    config_v2 = PipelineConfig(
        pipeline_name="test_pipe",
        owner="test_owner",
        source=SourceConfig(type="csv", path=str(csv_file)),
        destination=DestinationConfig(schema="raw", table="test_pipe"),
        schedule=ScheduleConfig(cron="0 12 * * *"),  # different cron
        load_mode="append",
        transform=TransformConfig(),
        quality_checks=QualityChecksConfig(),
        notify=NotifyConfig(),
    )
    executor.execute(config_v2)

    rows = db.query(
        "SELECT * FROM metadata_config_history WHERE pipeline_name = ? "
        "ORDER BY recorded_at",
        ["test_pipe"],
    )
    assert len(rows) == 2


def test_changed_load_mode_creates_new_history_record(tmp_path, db, executor):
    """Changing load_mode from append to overwrite creates a new record."""
    csv_file = tmp_path / "lm_pipe.csv"
    csv_file.write_text("id,value\n1,hello\n")

    def _config(load_mode):
        return PipelineConfig(
            pipeline_name="lm_pipe",
            owner="test_owner",
            source=SourceConfig(type="csv", path=str(csv_file)),
            destination=DestinationConfig(schema="raw", table="lm_pipe"),
            schedule=ScheduleConfig(cron="0 8 * * *"),
            load_mode=load_mode,
            transform=TransformConfig(),
            quality_checks=QualityChecksConfig(),
            notify=NotifyConfig(),
        )

    executor.execute(_config("append"))
    executor.execute(_config("overwrite"))

    rows = db.query(
        "SELECT * FROM metadata_config_history WHERE pipeline_name = ?",
        ["lm_pipe"],
    )
    assert len(rows) == 2


def test_different_pipelines_tracked_independently(tmp_path, db, executor):
    """Each pipeline has its own independent config history."""
    executor.execute(_make_config(tmp_path, name="pipe_a"))
    executor.execute(_make_config(tmp_path, name="pipe_b"))

    rows_a = db.query(
        "SELECT * FROM metadata_config_history WHERE pipeline_name = ?",
        ["pipe_a"],
    )
    rows_b = db.query(
        "SELECT * FROM metadata_config_history WHERE pipeline_name = ?",
        ["pipe_b"],
    )
    assert len(rows_a) == 1
    assert len(rows_b) == 1


def test_track_config_version_returns_true_on_new_config(tmp_path, db):
    """track_config_version returns True when a new config is stored."""
    result = db.track_config_version("new_pipe", "abc123def456abcd", '{"key": "val"}')
    assert result is True


def test_track_config_version_returns_false_on_unchanged_config(tmp_path, db):
    """track_config_version returns False when config hash is unchanged."""
    db.track_config_version("same_pipe", "abc123def456abcd", '{"key": "val"}')
    result = db.track_config_version("same_pipe", "abc123def456abcd", '{"key": "val"}')
    assert result is False
