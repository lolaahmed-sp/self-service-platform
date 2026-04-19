from __future__ import annotations

import pytest

from pipeline_platform.config_parser import (
    PipelineConfig,
    SourceConfig,
    DestinationConfig,
    ScheduleConfig,
    TransformConfig,
    QualityChecksConfig,
)
from pipeline_platform.pipeline_generator import PipelineExecutor
from pipeline_platform.warehouse.duckdb_client import DuckDBWarehouse
from main import _print_status


@pytest.fixture
def db(tmp_path):
    return DuckDBWarehouse(db_path=str(tmp_path / "test.duckdb"))


@pytest.fixture
def executor(db):
    return PipelineExecutor(warehouse=db)


def _make_config(tmp_path, name: str) -> PipelineConfig:
    csv_file = tmp_path / f"{name}.csv"
    csv_file.write_text("customer_id,full_name\nC001,Ada\nC002,Tomi\n")
    return PipelineConfig(
        pipeline_name=name,
        owner="test_owner",
        source=SourceConfig(type="csv", path=str(csv_file)),
        destination=DestinationConfig(schema="raw", table=name),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
        quality_checks=QualityChecksConfig(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# STATUS OUTPUT TESTS
# ─────────────────────────────────────────────────────────────────────────────


def test_status_prints_pipeline_name(tmp_path, db, executor, capsys):
    """Pipeline name appears in status output."""
    executor.execute(_make_config(tmp_path, "status_pipe"))
    _print_status(db)
    captured = capsys.readouterr()
    assert "status_pipe" in captured.out


def test_status_shows_success_indicator(tmp_path, db, executor, capsys):
    """A successful pipeline shows ✓ in status output."""
    executor.execute(_make_config(tmp_path, "success_pipe"))
    _print_status(db)
    captured = capsys.readouterr()
    assert "✓" in captured.out


def test_status_shows_failed_indicator(tmp_path, db, executor, capsys):
    """A failed pipeline shows ✗ in status output."""
    config = PipelineConfig(
        pipeline_name="fail_pipe",
        owner="test",
        source=SourceConfig(
            type="csv",
            path=str(tmp_path / "nonexistent.csv"),
        ),
        destination=DestinationConfig(schema="raw", table="fail_pipe"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
        quality_checks=QualityChecksConfig(),
    )
    executor.execute(config)
    _print_status(db)
    captured = capsys.readouterr()
    assert "✗" in captured.out


def test_status_shows_run_counts(tmp_path, db, executor, capsys):
    """Run count appears in status output."""
    config = _make_config(tmp_path, "count_pipe")
    executor.execute(config)
    executor.execute(config)
    _print_status(db)
    captured = capsys.readouterr()
    assert "2 total" in captured.out


def test_status_shows_owner(tmp_path, db, executor, capsys):
    """Owner name appears in status output."""
    executor.execute(_make_config(tmp_path, "owner_pipe"))
    _print_status(db)
    captured = capsys.readouterr()
    assert "test_owner" in captured.out


def test_status_shows_last_run_time(tmp_path, db, executor, capsys):
    """Last run timestamp appears in status output."""
    executor.execute(_make_config(tmp_path, "time_pipe"))
    _print_status(db)
    captured = capsys.readouterr()
    assert "2026" in captured.out


def test_status_handles_no_pipelines(db, capsys):
    """Status prints a friendly message when no pipelines are registered."""
    _print_status(db)
    captured = capsys.readouterr()
    assert "No pipelines registered" in captured.out


def test_status_shows_multiple_pipelines(tmp_path, db, executor, capsys):
    """All registered pipelines appear in status output."""
    executor.execute(_make_config(tmp_path, "pipe_alpha"))
    executor.execute(_make_config(tmp_path, "pipe_beta"))
    _print_status(db)
    captured = capsys.readouterr()
    assert "pipe_alpha" in captured.out
    assert "pipe_beta" in captured.out
