from __future__ import annotations

import pytest

from pipeline_platform.config_parser import (
    PipelineConfig,
    SourceConfig,
    DestinationConfig,
    ScheduleConfig,
    TransformConfig,
)
from pipeline_platform.metadata.registry import PipelineRegistry
from pipeline_platform.warehouse.duckdb_client import DuckDBWarehouse


# FIXTURES


@pytest.fixture
def db(tmp_path):
    """Isolated DuckDB — never touches the real warehouse.duckdb."""
    return DuckDBWarehouse(db_path=str(tmp_path / "test.duckdb"))


@pytest.fixture
def registry(db):
    reg = PipelineRegistry(db)
    reg.ensure_metadata_tables()
    return reg, db


# HELPER
# Builds PipelineConfig using the dataclasses from config_parser.
# register_pipeline() expects a PipelineConfig, not keyword args.


def _make_pipeline_config(
    name: str = "test_pipe",
    load_mode: str = "append",
) -> PipelineConfig:
    return PipelineConfig(
        pipeline_name=name,
        owner="test_owner",
        source=SourceConfig(
            type="csv",
            path=f"data_sources/{name}.csv",
        ),
        destination=DestinationConfig(
            schema="raw",
            table=name,
        ),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode=load_mode,
        transform=TransformConfig(),
    )


# TABLE ISOLATION


def test_fresh_database_has_no_pipeline_rows(registry):
    """tmp_path isolation — each test starts with a completely empty DB."""
    _, db = registry
    rows = db.query("SELECT * FROM metadata_pipelines")
    assert len(rows) == 0


# REGISTRATION


def test_register_pipeline_creates_row(registry):
    """Registering a pipeline writes one row to metadata_pipelines."""
    reg, db = registry
    reg.register_pipeline(_make_pipeline_config("pipe_a"))

    rows = db.query(
        "SELECT * FROM metadata_pipelines WHERE pipeline_name = ?",
        ["pipe_a"],
    )
    assert len(rows) == 1
    assert rows[0]["pipeline_name"] == "pipe_a"
    assert rows[0]["owner"] == "test_owner"
    assert rows[0]["source_type"] == "csv"
    assert rows[0]["load_mode"] == "append"


def test_register_pipeline_upserts_on_second_call(registry):
    """Calling register_pipeline twice on the same name must not create duplicates."""
    reg, db = registry

    reg.register_pipeline(_make_pipeline_config("pipe_b", load_mode="append"))
    reg.register_pipeline(_make_pipeline_config("pipe_b", load_mode="overwrite"))

    rows = db.query(
        "SELECT * FROM metadata_pipelines WHERE pipeline_name = ?",
        ["pipe_b"],
    )
    assert len(rows) == 1
    assert rows[0]["load_mode"] == "overwrite"


def test_register_multiple_pipelines_creates_separate_rows(registry):
    """Each unique pipeline name gets its own row."""
    reg, db = registry

    for name in ("alpha", "beta", "gamma"):
        reg.register_pipeline(_make_pipeline_config(name))

    rows = db.query(
        "SELECT pipeline_name FROM metadata_pipelines ORDER BY pipeline_name"
    )
    names = [r["pipeline_name"] for r in rows]
    assert names == ["alpha", "beta", "gamma"]


def test_register_stores_correct_destination(registry):
    """destination_schema and destination_table are stored correctly."""
    reg, db = registry
    reg.register_pipeline(_make_pipeline_config("dest_test"))

    rows = db.query(
        "SELECT destination_schema, destination_table "
        "FROM metadata_pipelines WHERE pipeline_name = ?",
        ["dest_test"],
    )
    assert rows[0]["destination_schema"] == "raw"
    assert rows[0]["destination_table"] == "dest_test"


# STATUS UPDATES


def test_update_last_run_status_to_success(registry):
    reg, db = registry
    reg.register_pipeline(_make_pipeline_config("pipe_c"))
    reg.update_last_run_status("pipe_c", "SUCCESS")

    rows = db.query(
        "SELECT last_status FROM metadata_pipelines WHERE pipeline_name = ?",
        ["pipe_c"],
    )
    assert rows[0]["last_status"] == "SUCCESS"


def test_update_last_run_status_to_failed(registry):
    reg, db = registry
    reg.register_pipeline(_make_pipeline_config("pipe_d"))
    reg.update_last_run_status("pipe_d", "FAILED")

    rows = db.query(
        "SELECT last_status FROM metadata_pipelines WHERE pipeline_name = ?",
        ["pipe_d"],
    )
    assert rows[0]["last_status"] == "FAILED"


def test_update_last_run_status_overwrites_previous(registry):
    """A second status update replaces the first — no duplicates."""
    reg, db = registry
    reg.register_pipeline(_make_pipeline_config("pipe_e"))

    reg.update_last_run_status("pipe_e", "SUCCESS")
    reg.update_last_run_status("pipe_e", "FAILED")

    rows = db.query(
        "SELECT last_status FROM metadata_pipelines WHERE pipeline_name = ?",
        ["pipe_e"],
    )
    assert len(rows) == 1
    assert rows[0]["last_status"] == "FAILED"
