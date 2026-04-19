from __future__ import annotations

import pytest
import pandas as pd

from pipeline_platform.config_parser import (
    PipelineConfig,
    SourceConfig,
    DestinationConfig,
    ScheduleConfig,
    TransformConfig,
    QualityChecksConfig,
    QualityCheckConfig,
)
from pipeline_platform.errors import DataQualityError
from pipeline_platform.pipeline_generator import PipelineExecutor
from pipeline_platform.quality.checker import run_quality_checks
from pipeline_platform.warehouse.duckdb_client import DuckDBWarehouse


# ─────────────────────────────────────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def db(tmp_path):
    return DuckDBWarehouse(db_path=str(tmp_path / "test.duckdb"))


@pytest.fixture
def executor(db):
    return PipelineExecutor(warehouse=db)


def _checks(*args) -> QualityChecksConfig:
    return QualityChecksConfig(enabled=True, checks=list(args))


def _check(name, column, rule, value=None) -> QualityCheckConfig:
    return QualityCheckConfig(name=name, column=column, rule=rule, value=value)


# ─────────────────────────────────────────────────────────────────────────────
# not_null rule
# ─────────────────────────────────────────────────────────────────────────────


def test_not_null_passes_when_no_nulls():
    df = pd.DataFrame({"customer_id": ["C001", "C002", "C003"]})
    checks = _checks(_check("no_nulls", "customer_id", "not_null"))
    run_quality_checks(df, checks)


def test_not_null_fails_when_nulls_present():
    df = pd.DataFrame({"customer_id": ["C001", None, "C003"]})
    checks = _checks(_check("no_nulls", "customer_id", "not_null"))
    with pytest.raises(DataQualityError, match="null"):
        run_quality_checks(df, checks)


def test_not_null_error_message_includes_column_name():
    df = pd.DataFrame({"full_name": [None, "Ada", None]})
    checks = _checks(_check("name_check", "full_name", "not_null"))
    with pytest.raises(DataQualityError, match="full_name"):
        run_quality_checks(df, checks)


# ─────────────────────────────────────────────────────────────────────────────
# unique rule
# ─────────────────────────────────────────────────────────────────────────────


def test_unique_passes_when_all_distinct():
    df = pd.DataFrame({"order_id": [1001, 1002, 1003]})
    checks = _checks(_check("unique_order", "order_id", "unique"))
    run_quality_checks(df, checks)


def test_unique_fails_when_duplicates_present():
    df = pd.DataFrame({"order_id": [1001, 1001, 1002]})
    checks = _checks(_check("unique_order", "order_id", "unique"))
    with pytest.raises(DataQualityError, match="duplicate"):
        run_quality_checks(df, checks)


# ─────────────────────────────────────────────────────────────────────────────
# greater_than rule
# ─────────────────────────────────────────────────────────────────────────────


def test_greater_than_passes_when_all_above_threshold():
    df = pd.DataFrame({"amount": [10.0, 20.0, 150.50]})
    checks = _checks(_check("positive_amount", "amount", "greater_than", 0))
    run_quality_checks(df, checks)


def test_greater_than_fails_when_value_at_or_below_threshold():
    df = pd.DataFrame({"amount": [10.0, 0.0, 150.50]})
    checks = _checks(_check("positive_amount", "amount", "greater_than", 0))
    with pytest.raises(DataQualityError, match="greater than"):
        run_quality_checks(df, checks)


# ─────────────────────────────────────────────────────────────────────────────
# min_rows rule
# ─────────────────────────────────────────────────────────────────────────────


def test_min_rows_passes_when_enough_rows():
    df = pd.DataFrame({"id": [1, 2, 3]})
    checks = _checks(_check("enough_rows", "id", "min_rows", 1))
    run_quality_checks(df, checks)


def test_min_rows_fails_when_dataframe_empty():
    df = pd.DataFrame({"id": []})
    checks = _checks(_check("enough_rows", "id", "min_rows", 1))
    with pytest.raises(DataQualityError, match="rows"):
        run_quality_checks(df, checks)


# ─────────────────────────────────────────────────────────────────────────────
# column not found
# ─────────────────────────────────────────────────────────────────────────────


def test_check_fails_when_column_not_in_dataframe():
    df = pd.DataFrame({"order_id": [1, 2]})
    checks = _checks(_check("check", "nonexistent_col", "not_null"))
    with pytest.raises(DataQualityError, match="not found"):
        run_quality_checks(df, checks)


# ─────────────────────────────────────────────────────────────────────────────
# disabled checks — no-op
# ─────────────────────────────────────────────────────────────────────────────


def test_disabled_checks_do_not_run():
    df = pd.DataFrame({"customer_id": [None, None, None]})
    checks = QualityChecksConfig(
        enabled=False,
        checks=[_check("no_nulls", "customer_id", "not_null")],
    )
    run_quality_checks(df, checks)


def test_no_checks_configured_does_not_raise():
    df = pd.DataFrame({"id": [1, 2]})
    checks = QualityChecksConfig(enabled=True, checks=[])
    run_quality_checks(df, checks)


# ─────────────────────────────────────────────────────────────────────────────
# multiple checks — all failures reported together
# ─────────────────────────────────────────────────────────────────────────────


def test_multiple_failures_reported_in_single_error():
    df = pd.DataFrame(
        {
            "customer_id": [None, None],
            "order_id": [1, 1],
        }
    )
    checks = _checks(
        _check("no_nulls", "customer_id", "not_null"),
        _check("unique_order", "order_id", "unique"),
    )
    with pytest.raises(DataQualityError) as exc_info:
        run_quality_checks(df, checks)
    error_text = str(exc_info.value)
    assert "customer_id" in error_text
    assert "order_id" in error_text


# ─────────────────────────────────────────────────────────────────────────────
# end-to-end integration
# ─────────────────────────────────────────────────────────────────────────────


def test_quality_failure_logs_failed_run(tmp_path, db, executor):
    """When a quality check fails the run is logged as FAILED."""
    csv_file = tmp_path / "bad_data.csv"
    csv_file.write_text(
        "customer_id,full_name,country,created_at\n"
        ",Ada Ibrahim,Nigeria,2026-02-20T08:00:00\n"
        "C002,Tomi Adebayo,Nigeria,2026-02-21T09:15:00\n"
    )

    config = PipelineConfig(
        pipeline_name="dq_fail_test",
        owner="test",
        source=SourceConfig(type="csv", path=str(csv_file)),
        destination=DestinationConfig(schema="raw", table="dq_fail_test"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
        quality_checks=QualityChecksConfig(
            enabled=True,
            checks=[
                QualityCheckConfig(
                    name="no_null_customer_id",
                    column="customer_id",
                    rule="not_null",
                )
            ],
        ),
    )

    result = executor.execute(config)

    assert result["status"] == "failed"
    assert "customer_id" in result["error_message"]

    runs = db.query(
        "SELECT status FROM metadata_pipeline_runs WHERE pipeline_name = ?",
        ["dq_fail_test"],
    )
    assert runs[0]["status"] == "FAILED"


def test_quality_pass_allows_pipeline_to_succeed(tmp_path, db, executor):
    """When all quality checks pass the pipeline succeeds."""
    csv_file = tmp_path / "good_data.csv"
    csv_file.write_text(
        "customer_id,full_name,country,created_at\n"
        "C001,Ada Ibrahim,Nigeria,2026-02-20T08:00:00\n"
        "C002,Tomi Adebayo,Nigeria,2026-02-21T09:15:00\n"
    )

    config = PipelineConfig(
        pipeline_name="dq_pass_test",
        owner="test",
        source=SourceConfig(type="csv", path=str(csv_file)),
        destination=DestinationConfig(schema="raw", table="dq_pass_test"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
        quality_checks=QualityChecksConfig(
            enabled=True,
            checks=[
                QualityCheckConfig(
                    name="no_null_customer_id",
                    column="customer_id",
                    rule="not_null",
                ),
                QualityCheckConfig(
                    name="unique_customer_id",
                    column="customer_id",
                    rule="unique",
                ),
            ],
        ),
    )

    result = executor.execute(config)

    assert result["status"] == "success"
    assert result["rows_loaded"] == 2
